package job

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/buildkite/agent/v4/env"
	"github.com/buildkite/agent/v4/internal/process"
	"github.com/buildkite/agent/v4/internal/shell"
	"github.com/buildkite/agent/v4/jobapi"
)

// agentErrorCaptureEnabled reports whether the job opted in to automatic
// reports of agent-observed failures, and the Local Job API can deliver them.
func agentErrorCaptureEnabled(environ *env.Environment) bool {
	return environ.GetString("BUILDKITE_CAPTURE_AGENT_ERRORS", "") == "true" &&
		environ.GetString("BUILDKITE_AGENT_JOB_API_CAPTURE_ERROR", "") == "true"
}

// captureJobError reports a failure the agent observed while running the job,
// such as a failed hook. The Local Job API redacts registered secrets.
// Delivery is best-effort and never changes the job's outcome.
func captureJobError(ctx context.Context, sh *shell.Shell, code, message string, details map[string]any) {
	// Failures after cancellation are consequences of it, not separate errors.
	if !agentErrorCaptureEnabled(sh.Env) || sh.Env.GetString("BUILDKITE_JOB_CANCELLED", "") == "true" {
		return
	}
	now := time.Now()
	deliverError(ctx, sh, jobapi.CapturedError{Code: code, Message: message, Context: details, Timestamp: &now})
}

// captureHookError reports a hook that failed or could not be run, with the
// end of its output, so the report explains the failure without the job log.
func (e *Executor) captureHookError(ctx context.Context, hookCfg HookConfig, hookName string, err error) {
	if err == nil {
		return
	}
	details := map[string]any{"hook": hookCfg.Name, "scope": hookCfg.Scope, "path": e.hookDisplayPath(hookCfg)}
	if hookCfg.PluginName != "" {
		details["plugin"] = hookCfg.PluginName
	}
	// Say where the hook lives when it is not in the repository being built.
	where := map[string]string{
		HookScopeAgent:  " It is installed on the agent, not in the repository.",
		HookScopePlugin: " It is part of the plugin, not the repository.",
	}[hookCfg.Scope]
	message := fmt.Sprintf("The %s hook at %s could not be run: %v.%s", hookName, details["path"], err, where)
	if addExitDetails(details, err) {
		message = fmt.Sprintf("The %s hook at %s exited with status %d.%s", hookName, details["path"], shell.ExitCode(err), where)
	}
	captureJobError(ctx, e.shell, "hook_failed", e.withRecentOutput(message, details), details)
}

// hookDisplayPath shows repository hooks relative to the checkout and plugin
// hooks relative to the plugin, which is how a pipeline author finds them.
func (e *Executor) hookDisplayPath(hookCfg HookConfig) string {
	switch hookCfg.Scope {
	case HookScopeRepository:
		checkout := e.shell.Env.GetString("BUILDKITE_BUILD_CHECKOUT_PATH", "")
		if rel, err := filepath.Rel(checkout, hookCfg.Path); checkout != "" && err == nil && !strings.HasPrefix(rel, "..") {
			return filepath.ToSlash(rel)
		}
	case HookScopePlugin:
		return "hooks/" + filepath.Base(hookCfg.Path)
	}
	return hookCfg.Path
}

// captureCommandError reports a command that failed or could not be run,
// whether run by default or by a command hook, with the end of its output.
func (e *Executor) captureCommandError(ctx context.Context, err error) {
	details := map[string]any{}
	// Long scripts are already in the step definition.
	if command := strings.TrimSpace(e.Command); command != "" && len(command) <= 256 {
		details["command"] = command
	}
	switch {
	case addExitDetails(details, err):
		if scope := e.commandHookScope(); scope != "" {
			details["command_hook"] = scope
		}
		message := fmt.Sprintf("The command exited with status %d.", shell.ExitCode(err))
		if signal, ok := details["signal"]; ok {
			message = fmt.Sprintf("The command was terminated by signal %s.", signal)
			if signal == "SIGKILL" {
				message += " The operating system may have killed it for using too much memory."
			}
		}
		captureJobError(ctx, e.shell, "command_failed", e.withRecentOutput(message, details), details)
	case errors.Is(err, errNoCommand):
		captureJobError(ctx, e.shell, "command_missing", err.Error(), nil)
	case errors.Is(err, errCommandEvalDisabled), errors.Is(err, errCommandOutsideRepository):
		captureJobError(ctx, e.shell, "command_eval_disabled", err.Error(), details)
	default:
		captureJobError(ctx, e.shell, "command_not_run", jobapi.CapturedErrorMessage(err.Error(), "The command could not be run.", details), details)
	}
}

// commandHookScope reports which command hook replaced the step's command, if
// any, in the order runCommand chooses one.
func (e *Executor) commandHookScope() string {
	switch {
	case e.hasPluginHook("command"):
		return HookScopePlugin
	case e.hasLocalHook("command"):
		return HookScopeRepository
	case e.hasGlobalHook("command"):
		return HookScopeAgent
	}
	return ""
}

// withRecentOutput appends the end of the redacted output from the hook,
// command, or plugin checkout that just failed, using whatever room the
// detail budget leaves after the summary and context.
func (e *Executor) withRecentOutput(summary string, details map[string]any) string {
	// Release output a redactor is holding back in case it starts a secret.
	_ = e.redactors.Flush()
	size, err := jobapi.CapturedErrorDetailsSize(summary, details)
	if err != nil {
		return summary
	}
	const heading = "\n\nLast lines of output:\n"
	recent := e.outputTail.tail(jobapi.MaxCapturedErrorDetail - size - len(heading))
	if recent == "" {
		return summary
	}
	return summary + heading + recent
}

// addExitDetails adds the exit status, and any terminating signal, of a
// process that exited unsuccessfully. It reports whether err was such an exit.
func addExitDetails(details map[string]any, err error) bool {
	if !shell.IsExitError(err) {
		return false
	}
	details["exit_status"] = shell.ExitCode(err)
	if exitErr := new(exec.ExitError); errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			details["signal"] = process.SignalString(status.Signal())
		}
	}
	return true
}

func deliverError(ctx context.Context, sh *shell.Shell, report jobapi.CapturedError) {
	if ctx.Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	client, err := jobapi.NewClient(ctx, sh.Env.GetString("BUILDKITE_AGENT_JOB_API_SOCKET", ""), sh.Env.GetString("BUILDKITE_AGENT_JOB_API_TOKEN", ""))
	if err == nil {
		err = client.CaptureError(ctx, &report)
	}
	if err != nil {
		// Transport errors can contain upstream response bodies or socket paths.
		sh.Warningf("Could not capture job error %q", report.Code)
	}
}
