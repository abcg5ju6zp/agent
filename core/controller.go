package core

import (
	"context"
	"errors"

	"github.com/buildkite/agent/v4/api"
	"github.com/buildkite/agent/v4/logger"
	"github.com/buildkite/agent/v4/version"
)

// Controller is a client for the Buildkite Agent API. It is useful for
// implementing completely custom job behaviour within process (instead of
// having to execute hooks).
//
// By default a Controller registers ignored from dispatches and can only
// obtain jobs via [Controller.AcquireJob]. Create it with [WithScheduling]
// and run [Controller.RunDispatch] to also participate in regular
// scheduling, like an ordinary agent.
type Controller struct {
	client *Client
	config *controllerConfig

	// agent is the agent registration record.
	agent *api.AgentRegisterResponse

	// sched runs the scheduling loops, and is non-nil only when the
	// Controller was created with [WithScheduling].
	sched *scheduler
}

// NewController creates and registers a new agent with Buildkite.
// By default, only Acquire Job is supported; pass [WithScheduling] to
// register a dispatchable agent and use [Controller.RunDispatch].
// ctx is only used for agent registration and connection.
func NewController(ctx context.Context, regToken, agentName string, tags []string, opts ...ControllerOption) (*Controller, error) {
	// Some of these are redundant by Go zero-value defaults, but it spells out
	// what the defaults are.
	cfg := &controllerConfig{
		logger:            logger.Discard,
		retrySleepFunc:    nil,
		endpoint:          "https://agent-edge.buildkite.com/v3",
		userAgent:         version.UserAgent(),
		debugHTTP:         false,
		allowHTTP2:        true,
		priority:          "",
		scriptEvalEnabled: true,
	}
	for _, o := range opts {
		o(cfg)
	}

	apiClient := api.NewClient(cfg.logger, api.Config{
		Token: regToken,

		Endpoint:     cfg.endpoint,
		UserAgent:    cfg.userAgent,
		DisableHTTP2: !cfg.allowHTTP2,
		DebugHTTP:    cfg.debugHTTP,
	})

	controller := &Controller{
		config: cfg,
		client: &Client{
			APIClient:      apiClient,
			Logger:         cfg.logger,
			RetrySleepFunc: cfg.retrySleepFunc,
		},
	}

	reg, err := controller.client.Register(ctx, api.AgentRegisterRequest{
		Name: agentName,
		// In scheduling mode the agent participates in dispatches like an
		// ordinary agent; otherwise it is ignored and only acquires jobs by ID.
		IgnoreInDispatches: !cfg.scheduling,
		ScriptEvalEnabled:  cfg.scriptEvalEnabled,
		Priority:           cfg.priority,
		Tags:               tags,
		Features:           []string{"agent-core"},
	})
	if err != nil {
		return nil, err
	}
	controller.agent = reg
	controller.client.APIClient = apiClient.FromAgentRegisterResponse(reg)

	if err := controller.client.Connect(ctx); err != nil {
		return nil, err
	}

	if cfg.scheduling {
		controller.sched = newScheduler(controller.client, reg, cfg.logger, cfg.noDelays)
	}

	return controller, nil
}

// RunDispatch runs the regular scheduling loops: heartbeats, job dispatch via
// streaming when the control plane supports it (with automatic fallback to
// polling pings otherwise), and handling of pause, resume and disconnect
// instructions.
//
// Each dispatched job is accepted and then passed to handler.Job exactly
// once. No new job is delivered while paused or while a previous job is still
// running. Transient connection interruptions are retried against the same
// registered agent; RunDispatch returns after a disconnect instruction, when
// its context is cancelled, or when [Controller.Close] is called.
//
// RunDispatch must be called at most once, and only on a Controller created
// with [WithScheduling].
func (c *Controller) RunDispatch(ctx context.Context, handler DispatchHandler) error {
	if c.sched == nil {
		return ErrSchedulingNotEnabled
	}
	return c.sched.run(ctx, handler)
}

// Close stops receiving new work and disconnects the agent.
//
// In scheduling mode it performs a graceful shutdown: scheduling loops stop
// accepting new jobs, and Close waits for any job already delivered to
// DispatchHandler.Job to finish before disconnecting, bounded by ctx. The
// in-flight job's context (derived from the RunDispatch context) is not
// cancelled by Close.
func (c *Controller) Close(ctx context.Context) error {
	if c.sched != nil {
		c.sched.internalStop()
		// Don't fail to disconnect just because the in-flight job outlasts
		// the shutdown deadline; report both.
		if err := c.sched.wait(ctx); err != nil {
			return errors.Join(err, c.client.Disconnect(ctx))
		}
	}
	return c.client.Disconnect(ctx)
}

// AcquireJob acquires a specific job from Buildkite.
// It doesn't run the job - the caller is responsible for that.
func (c *Controller) AcquireJob(ctx context.Context, jobID string) (*api.Job, error) {
	return c.client.AcquireJob(ctx, jobID)
}

// NewJobController creates a new job controller for a job.
func (c *Controller) NewJobController(job *api.Job) *JobController {
	return NewJobController(c.client, job)
}
