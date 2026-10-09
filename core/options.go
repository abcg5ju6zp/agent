package core

import (
	"time"

	"github.com/buildkite/agent/v4/logger"
)

type controllerConfig struct {
	logger         logger.Logger
	retrySleepFunc func(time.Duration)

	// Directory used to persist recoverable per-job delivery sessions. Empty
	// means use the platform default cache directory.
	jobStateDir string

	// Controller creation options - ignored for more specific functions.
	endpoint          string
	userAgent         string
	debugHTTP         bool
	allowHTTP2        bool
	priority          string
	scriptEvalEnabled bool
}

// jobControllerOptions configures a JobController's persistent session.
type jobControllerOptions struct {
	// stateDir is the directory in which per-job session state and chunk
	// write-ahead logs are stored. Empty means use the platform default cache
	// directory.
	stateDir string

	// staleLockTimeout bounds how long a new controller waits for a live
	// session owner to release its lock before fencing the owner and
	// continuing recovery without the lock.
	staleLockTimeout time.Duration
}

// JobControllerOption is a functional option for a job controller.
type JobControllerOption func(*jobControllerOptions)

// ControllerOption is a functional option for setting optional behaviour.
type ControllerOption func(*controllerConfig)

// WithLogger enables logging through a particular logger.
// Defaults to [logger.Discard].
func WithLogger(l logger.Logger) ControllerOption {
	return func(c *controllerConfig) {
		c.logger = l
	}
}

// WithRetrySleepFunc is used to override the inter-retry sleep in roko.
// This is mainly useful for unit tests. Defaults to nil (default sleep).
func WithRetrySleepFunc(f func(time.Duration)) ControllerOption {
	return func(c *controllerConfig) {
		c.retrySleepFunc = f
	}
}

// WithEndpoint allows overriding the API endpoint (base URL).
// Defaults to "https://agent-edge.buildkite.com/v3".
func WithEndpoint(endpoint string) ControllerOption {
	return func(c *controllerConfig) {
		c.endpoint = endpoint
	}
}

// WithUserAgent allows overriding the user agent.
// Defaults to the value retuned from [version.UserAgent].
func WithUserAgent(userAgent string) ControllerOption {
	return func(c *controllerConfig) {
		c.userAgent = userAgent
	}
}

// WithDebugHTTP can be used to enable HTTP debug logs.
// Only applies to agent creation. Defaults to false.
func WithDebugHTTP(debug bool) ControllerOption {
	return func(c *controllerConfig) {
		c.debugHTTP = debug
	}
}

// WithAllowHTTP can be used to change whether HTTP/2 is allowed.
// Only applies to agent creation. Defaults to true.
func WithAllowHTTP2(allow bool) ControllerOption {
	return func(c *controllerConfig) {
		c.allowHTTP2 = allow
	}
}

// WithPriority sets the agent priority value. Defaults to the empty string.
func WithPriority(priority string) ControllerOption {
	return func(c *controllerConfig) {
		c.priority = priority
	}
}

// WithScriptEvalEnabled sets the ScriptEvalEnabled registration parameter.
// Defaults to true.
func WithScriptEvalEnabled(enabled bool) ControllerOption {
	return func(c *controllerConfig) {
		c.scriptEvalEnabled = enabled
	}
}

// WithJobStateDir sets the directory used to persist recoverable per-job
// delivery sessions (chunk delivery state, write-ahead logs, and start/finish
// state). Defaults to a "buildkite-agent/job-sessions" directory under the
// platform user cache directory.
func WithJobStateDir(dir string) ControllerOption {
	return func(c *controllerConfig) {
		c.jobStateDir = dir
	}
}

// WithStateDir sets the directory used to persist this job controller's
// recoverable delivery session. Defaults to a "buildkite-agent/job-sessions"
// directory under the platform user cache directory.
func WithStateDir(dir string) JobControllerOption {
	return func(o *jobControllerOptions) {
		o.stateDir = dir
	}
}

// WithStaleLockTimeout sets how long a newly created job controller waits for a
// live session owner to release its lock before fencing the owner and
// continuing recovery without the lock. Defaults to 10s. Mainly useful for
// unit tests.
func WithStaleLockTimeout(d time.Duration) JobControllerOption {
	return func(o *jobControllerOptions) {
		o.staleLockTimeout = d
	}
}
