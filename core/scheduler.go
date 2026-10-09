package core

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"github.com/buildkite/agent/v4/api"
	agentedgev1 "github.com/buildkite/agent/v4/api/proto/gen"
	"github.com/buildkite/agent/v4/logger"
	"github.com/buildkite/roko"
)

// DispatchHandler receives jobs and control-plane instructions while a
// Controller created with [WithScheduling] is running in regular scheduling
// mode (see [Controller.RunDispatch]).
type DispatchHandler struct {
	// Job is called exactly once for each job dispatched to the agent, after
	// the job has been accepted from the control plane. It is never called
	// concurrently with itself: the scheduler does not accept another
	// dispatched job until Job returns, and it is never called while the agent
	// is paused.
	//
	// The context passed to Job is derived from the context passed to
	// RunDispatch. A graceful [Controller.Close] does not cancel it, so a job
	// already in progress can run to completion (and report its result) before
	// disconnect; cancellation of the RunDispatch context, however, cancels
	// the job as well.
	//
	// A returned error is logged; it does not stop the scheduler. Use
	// [Controller.NewJobController] to start, log and finish the job.
	Job func(ctx context.Context, job *api.Job) error

	// Pause, if non-nil, is called once when the control plane instructs the
	// agent to pause (repeated pause instructions while already paused do not
	// invoke it again). While paused, Job is not called. The callback should
	// return promptly.
	Pause func(reason string)

	// Resume, if non-nil, is called when the control plane lifts a pause.
	// The callback should return promptly.
	Resume func()

	// Disconnect, if non-nil, is called when the control plane instructs the
	// agent to disconnect. After it is called, RunDispatch returns. The
	// caller is still responsible for calling [Controller.Close] to send the
	// disconnect request. The callback should return promptly.
	Disconnect func(reason string)
}

// Sentinel errors returned by [Controller.RunDispatch].
var (
	// ErrSchedulingNotEnabled is returned by RunDispatch if the Controller
	// was not created with [WithScheduling].
	ErrSchedulingNotEnabled = errors.New("scheduling mode is not enabled; create the controller with WithScheduling")

	// ErrDispatchAlreadyStarted is returned if RunDispatch is called more
	// than once.
	ErrDispatchAlreadyStarted = errors.New("dispatch loop has already been started")

	errDispatchMissingJobHandler = errors.New("DispatchHandler.Job is required in scheduling mode")
)

const (
	actorPingLoop  = "ping"
	actorDebouncer = "debouncer"

	defaultPingInterval      = 5 * time.Second
	defaultHeartbeatInterval = 30 * time.Second
)

// Internal error values that should not escape to the caller.
var (
	errInternalStop  = errors.New("stop")
	errInternalBreak = errors.New("break")
)

type unrecoverableError struct {
	action   string
	response *api.Response
	err      error
}

func (e *unrecoverableError) Error() string {
	status := "unknown"
	if e.response != nil {
		status = e.response.Status
	}
	return fmt.Sprintf("%s failed with unrecoverable status: %s, message: %q", e.action, status, e.err)
}

func (e *unrecoverableError) Unwrap() error { return e.err }

// See https://connectrpc.com/docs/protocol/#error-codes
var codeUnrecoverable = map[connect.Code]bool{
	connect.CodeUnauthenticated:  true, // invalid credentials
	connect.CodePermissionDenied: true, // not authorized
	connect.CodeUnimplemented:    true, // RPC not supported
	// CodeInternal is intentionally NOT here: it is also produced by
	// transient HTTP/2 transport errors that ConnectRPC maps to CodeInternal.
	// Unavailable (429, 502, 503, 504) and Unknown (other HTTP statuses) are
	// implicitly recoverable.
}

func isUnrecoverable(err error) bool {
	var u *unrecoverableError
	if errors.As(err, &u) {
		return true
	}
	return codeUnrecoverable[connect.CodeOf(err)]
}

// scheduler runs the regular-agent scheduling loops against the control plane:
// a heartbeat loop, a streaming dispatch loop with automatic fallback to a
// polling ping loop, a debouncer between them, and a single action loop that
// serialises job delivery.
type scheduler struct {
	client *Client
	agent  *api.AgentRegisterResponse
	logger logger.Logger

	handler  DispatchHandler
	noDelays bool

	// stop is closed once to ask every loop to stop.
	stopOnce sync.Once
	stop     chan struct{}

	// running guards against RunDispatch being called more than once.
	running atomic.Bool
	// done is closed when every loop has stopped, including any in-flight
	// job delivery, so it can be waited on by a graceful Close.
	done chan struct{}

	stateMtx sync.Mutex
	lastPing time.Time
}

func newScheduler(c *Client, agent *api.AgentRegisterResponse, l logger.Logger, noDelays bool) *scheduler {
	return &scheduler{
		client:   c,
		agent:    agent,
		logger:   l,
		noDelays: noDelays,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

func (s *scheduler) internalStop() {
	s.stopOnce.Do(func() { close(s.stop) })
}

// run starts the scheduling loops and blocks until they have all stopped
// (disconnect instruction, context cancellation, or Close). It must be called
// at most once.
func (s *scheduler) run(runCtx context.Context, h DispatchHandler) error {
	if h.Job == nil {
		return errDispatchMissingJobHandler
	}
	if !s.running.CompareAndSwap(false, true) {
		return ErrDispatchAlreadyStarted
	}
	s.handler = h

	defer close(s.done)

	// loopCtx drives the loops. Unlike runCtx (which is passed to delivered
	// jobs), it is also cancelled by a graceful stop, so the loops wind down
	// promptly while an in-progress job is left alone.
	loopCtx, cancelLoops := context.WithCancel(runCtx)
	defer cancelLoops()
	go func() {
		select {
		case <-s.stop:
			cancelLoops()
		case <-loopCtx.Done():
		}
	}()

	// The baton avoids the ping loop pinging while the streaming loop is
	// healthy, but lets the ping loop take over quickly when it isn't.
	bat := newBaton()

	// Channels connecting the loops, mirroring the main agent worker.
	fromPingLoopCh := make(chan actionMessage)      // ping loop -> action loop
	fromStreamingLoopCh := make(chan actionMessage) // streaming loop -> debouncer
	fromDebouncerCh := make(chan actionMessage)     // debouncer -> action loop

	// Auto mode: run the polling ping loop, the streaming ping loop and the
	// debouncer together. The streaming loop is preferred while healthy; the
	// ping loop covers (and permanently takes over) when streaming is
	// unavailable.
	pingLoop := func() error {
		return s.runPingLoop(loopCtx, bat, fromPingLoopCh)
	}
	streamingLoop := func() error {
		err := s.runStreamingPingLoop(loopCtx, fromStreamingLoopCh)
		if err != nil {
			// In auto mode, any streaming-loop failure (including an
			// unrecoverable one such as the server not implementing the
			// streaming RPC) results in a permanent fallback to polling,
			// rather than terminating scheduling. If the credentials are
			// invalid, the ping loop will discover that on its next ping.
			s.logger.Infof("Streaming dispatch is unavailable, permanently falling back to polling-based pings (the underlying error was: %v)", err)
		}
		return nil
	}
	debouncerLoop := func() error {
		return s.runDebouncer(loopCtx, bat, fromDebouncerCh, fromStreamingLoopCh)
	}
	actionLoop := func() error {
		return s.runActionLoop(runCtx, loopCtx, fromPingLoopCh, fromDebouncerCh)
	}

	heartbeatLoop := func() error {
		return s.runHeartbeatLoop(loopCtx)
	}

	loops := []func() error{pingLoop, streamingLoop, debouncerLoop, actionLoop, heartbeatLoop}

	// The debouncer starts holding the baton (streaming is assumed healthy
	// until proven otherwise), which keeps the ping loop blocked.
	<-bat.Acquire()
	bat.Acquired(actorDebouncer)

	loopErrs := make([]error, len(loops))
	var wg sync.WaitGroup
	for i, loop := range loops {
		wg.Go(func() { loopErrs[i] = loop() })
	}
	wg.Wait()

	// If the caller cancelled the RunDispatch context, surface that rather
	// than nil (the loops themselves return nil on context cancellation).
	if err := runCtx.Err(); err != nil {
		return err
	}
	return errors.Join(loopErrs...)
}

// wait blocks until the dispatch loops have fully stopped, or ctx ends.
func (s *scheduler) wait(ctx context.Context) error {
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runHeartbeatLoop periodically heartbeats so the control plane considers the
// agent alive. Unrecoverable heartbeat failures stop scheduling.
func (s *scheduler) runHeartbeatLoop(ctx context.Context) error {
	heartbeatInterval := time.Duration(s.agent.HeartbeatInterval) * time.Second
	if heartbeatInterval <= 0 {
		heartbeatInterval = defaultHeartbeatInterval
	}
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()
	for {
		select {
		case <-heartbeatTicker.C:
			if err := s.heartbeat(ctx); err != nil {
				if isUnrecoverable(err) {
					s.logger.Errorf("%s", err)
					s.internalStop()
					return err
				}
				s.logger.Warnf("Failed to heartbeat (%s); will try again in %v", err, heartbeatInterval)
			}
		case <-s.stop:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *scheduler) heartbeat(ctx context.Context) error {
	r := roko.NewRetrier(
		roko.WithMaxAttempts(10),
		roko.WithStrategy(roko.Constant(5*time.Second)),
		roko.WithSleepFunc(s.client.RetrySleepFunc),
	)
	beat, err := roko.DoFunc(ctx, r, func(r *roko.Retrier) (*api.Heartbeat, error) {
		b, resp, err := s.client.APIClient.Heartbeat(ctx)
		if err != nil {
			if resp != nil && !api.IsRetryableStatus(resp) && !api.IsRetryableError(err) {
				r.Break()
				return nil, &unrecoverableError{action: "Heartbeat", response: resp, err: err}
			}
			s.logger.Warnf("%s (%s)", err, r)
			return nil, err
		}
		return b, nil
	})
	if err != nil {
		return err
	}
	s.logger.Debugf("Heartbeat sent at %s and received at %s", beat.SentAt, beat.ReceivedAt)
	return nil
}

type actionMessage struct {
	// action is "", "pause" or "disconnect" ("" is resume/idle).
	action string
	jobID  string
	reason string

	// errCh is closed (or sent a result) once the action has been handled.
	errCh chan<- error

	// unhealthy is a private message from the streaming loop to the
	// debouncer indicating the stream should hand the baton to the ping loop.
	unhealthy bool
}

// runPingLoop runs the classical polling loop that pings Buildkite for work.
func (s *scheduler) runPingLoop(ctx context.Context, bat *baton, outCh chan<- actionMessage) error {
	s.logger.Debugf("[runPingLoop] Starting")
	defer s.logger.Debugf("[runPingLoop] Exiting")
	defer close(outCh)

	pingInterval := time.Duration(s.agent.PingInterval) * time.Second
	if pingInterval <= 0 {
		pingInterval = defaultPingInterval
	}
	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()
	skipWait := make(chan struct{}, 1)
	// Ping immediately on the first iteration.
	skipWait <- struct{}{}
	if s.noDelays {
		close(skipWait)
	}

	s.logger.Infof("Waiting for instructions...")

	for {
		select {
		case <-skipWait:
		case <-pingTicker.C:
		case <-s.stop:
			return nil
		case <-ctx.Done():
			return nil
		}

		if !s.noDelays {
			// Jitter within the interval to avoid agent synchronisation.
			select {
			case <-skipWait:
			case <-time.After(rand.N(pingInterval)):
			case <-s.stop:
				return nil
			case <-ctx.Done():
				return nil
			}
		}

		if err := s.pingLoopInner(ctx, bat, outCh, pingTicker, pingInterval, skipWait); err == errInternalStop {
			return nil
		} else if err != nil {
			return err
		}
	}
}

func (s *scheduler) pingLoopInner(ctx context.Context, bat *baton, outCh chan<- actionMessage, pingTicker *time.Ticker, pingInterval time.Duration, skipWait chan struct{}) error {
	// Wait for the baton. While the streaming loop is healthy the baton is
	// held by the debouncer and this blocks. It is always released after any
	// work completes, because the streaming loop is preferred.
	select {
	case <-bat.Acquire():
		bat.Acquired(actorPingLoop)
		defer bat.Release(actorPingLoop)
	case <-s.stop:
		return errInternalStop
	case <-ctx.Done():
		return errInternalStop
	}

	jobID, action, err := s.ping(ctx)
	if err != nil {
		if isUnrecoverable(err) {
			s.logger.Errorf("%v", err)
			return err
		}
		s.logger.Warnf("%v", err)
	}

	errCh := make(chan error)
	msg := actionMessage{action: action, jobID: jobID, errCh: errCh}
	select {
	case outCh <- msg:
	case <-s.stop:
		return errInternalStop
	case <-ctx.Done():
		return errInternalStop
	}

	select {
	case err := <-errCh:
		if err != nil || jobID == "" {
			return nil
		}
		if s.noDelays {
			return nil
		}
		// A job just ran; another is often immediately available, so ping
		// again soon but ensure a full interval passes after that ping.
		pingTicker.Reset(pingInterval)
		select {
		case skipWait <- struct{}{}:
		default:
		}
		return nil
	case <-s.stop:
		return errInternalStop
	case <-ctx.Done():
		return errInternalStop
	}
}

// ping performs a single polling ping and returns the job ID and action the
// control plane provided, if any.
func (s *scheduler) ping(ctx context.Context) (jobID, action string, err error) {
	ping, resp, pingErr := s.client.APIClient.Ping(ctx)
	if ping != nil {
		if ping.Message != "" {
			s.logger.Infof("%s", ping.Message)
		}
		action = ping.Action
	}

	if pingErr != nil {
		// Non-retryable statuses are fatal; transient failures are ignored,
		// the loop pings again after the interval. (Disconnect actions can
		// arrive on error responses, so handle the action above first.)
		if resp != nil && !api.IsRetryableStatus(resp) && !api.IsRetryableError(pingErr) {
			return "", action, &unrecoverableError{action: "Ping", response: resp, err: pingErr}
		}
		s.stateMtx.Lock()
		if s.lastPing.IsZero() {
			err = fmt.Errorf("failed to ping: %w (no successful ping yet)", pingErr)
		} else {
			err = fmt.Errorf("failed to ping: %w (last successful was %v ago)", pingErr, time.Since(s.lastPing))
		}
		s.stateMtx.Unlock()
		return "", action, err
	}

	s.stateMtx.Lock()
	s.lastPing = time.Now()
	s.stateMtx.Unlock()

	// Note: unlike the main agent worker, a ping-time endpoint switch is not
	// acted on; the endpoint from registration (and any stream reconnects) is
	// used throughout.

	// If there's no job, there's nothing to do. A paused agent isn't
	// dispatched jobs, but ignore the job if one is present anyway.
	if ping.Job == nil || action == "pause" {
		return "", action, nil
	}
	return ping.Job.ID, action, nil
}

// runStreamingPingLoop runs the streaming dispatch loop, reconnecting with
// fuzzed exponential backoff. A failure makes it unhealthy (the ping loop
// takes over); an unrecoverable failure returns an error so auto mode can fall
// back to polling permanently.
func (s *scheduler) runStreamingPingLoop(ctx context.Context, outCh chan<- actionMessage) error {
	s.logger.Debugf("[runStreamingPingLoop] Starting")
	defer s.logger.Debugf("[runStreamingPingLoop] Exiting")
	defer close(outCh)

	// The blocking Receive cannot be selected on, so a separate context ends
	// the stream on stop.
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	go func() {
		select {
		case <-s.stop:
			cancelStream()
		case <-ctx.Done():
		}
	}()

	// A closed skip channel makes all waits return immediately (tests).
	var skipWait <-chan struct{}
	if s.noDelays {
		ch := make(chan struct{})
		close(ch)
		skipWait = ch
	}

	const initialWindow = 1 * time.Second
	attempts := 0
	// firstMsg is re-armed only on a mode switch (stream -> ping -> stream),
	// so routine reconnects (e.g. after deadline-exceeded) don't re-log.
	firstMsg := true

	for {
		// Exponential backoff capped at initialWindow * 2^6 (64s).
		window := initialWindow << min(attempts, 6)
		windowEnd := time.After(window)
		attempts++

		jitter := rand.N(window)
		select {
		case <-skipWait:
		case <-time.After(jitter):
		case <-s.stop:
			return nil
		case <-ctx.Done():
			return nil
		}

		err := s.startStream(ctx, streamCtx, outCh, &attempts, &firstMsg)
		if err == errInternalStop {
			return nil
		}
		if err != nil {
			return err
		}

		// Wait out the remainder of the fuzz window before reconnecting.
		select {
		case <-skipWait:
		case <-windowEnd:
		case <-s.stop:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

// startStream opens one stream connection and processes its messages.
func (s *scheduler) startStream(ctx, streamCtx context.Context, outCh chan<- actionMessage, attempts *int, firstMsg *bool) error {
	stream, err := s.client.APIClient.StreamPings(streamCtx, s.agent.UUID)
	if err != nil {
		s.logger.Debugf("[runStreamingPingLoop] Connection to ping stream failed: %v", err)
		if isUnrecoverable(err) {
			return err
		}
		// Fast fallback to the ping loop while the stream keeps retrying.
		*firstMsg = true
		return s.sendStreamMessage(ctx, outCh, actionMessage{unhealthy: true})
	}

	for msg, streamErr := range stream {
		err := s.handleStreamMessage(ctx, outCh, msg, streamErr, attempts, firstMsg)
		if err == errInternalBreak {
			break
		}
		if err == errInternalStop {
			return errInternalStop
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *scheduler) handleStreamMessage(ctx context.Context, outCh chan<- actionMessage, msg *agentedgev1.StreamPingsResponse, streamErr error, attempts *int, firstMsg *bool) error {
	var amsg actionMessage
	switch {
	case streamErr != nil:
		s.logger.Debugf("[runStreamingPingLoop] Ping stream failed or ended: %v", streamErr)
		if isUnrecoverable(streamErr) {
			return streamErr
		}
		// A deadline-exceeded is an expected timeout; just reconnect.
		if connect.CodeOf(streamErr) == connect.CodeDeadlineExceeded {
			return errInternalBreak
		}
		amsg.unhealthy = true

	case msg == nil:
		s.logger.Debugf("[runStreamingPingLoop] Ping stream yielded a nil message; assuming it is broken")
		amsg.unhealthy = true

	default:
		if *firstMsg {
			s.logger.Infof("Ping stream connection established")
			*firstMsg = false
		}
		switch act := msg.Action.(type) {
		case *agentedgev1.StreamPingsResponse_Resume: // a.k.a. idle
			// nothing
		case *agentedgev1.StreamPingsResponse_Pause:
			amsg.action = "pause"
			amsg.reason = act.Pause.GetReason()
			if amsg.reason != "" {
				s.logger.Infof("Pause reason: %s", amsg.reason)
			}
		case *agentedgev1.StreamPingsResponse_Disconnect:
			amsg.action = "disconnect"
			amsg.reason = act.Disconnect.GetReason()
			if amsg.reason != "" {
				s.logger.Infof("Disconnect reason: %s", amsg.reason)
			}
		case *agentedgev1.StreamPingsResponse_JobAssigned:
			amsg.jobID = act.JobAssigned.GetJob().GetId()
			if amsg.jobID == "" {
				s.logger.Errorf("Ping stream yielded a JobAssigned message with an empty job ID; assuming the stream is broken")
				amsg.unhealthy = true
			}
		}
	}

	if err := s.sendStreamMessage(ctx, outCh, amsg); err != nil {
		return err
	}

	// On a disconnect instruction, do not stop the loops here: let the
	// action loop deliver the instruction to the caller and drive the
	// resulting shutdown, so the instruction cannot lose a race with stop.
	// Keep the stream (and its output channel) alive until the action loop
	// has processed the message and stopped everything.
	if amsg.action == "disconnect" {
		s.logger.Debugf("[runStreamingPingLoop] Disconnect instruction received; waiting for shutdown")
		select {
		case <-s.stop:
		case <-ctx.Done():
		}
		return errInternalStop
	}

	if amsg.unhealthy {
		// Re-arm the established log for the next successful connection.
		*firstMsg = true
		return errInternalBreak
	}
	// Healthy: reset the backoff counter.
	*attempts = 0
	return nil
}

func (s *scheduler) sendStreamMessage(ctx context.Context, outCh chan<- actionMessage, msg actionMessage) error {
	select {
	case outCh <- msg:
		return nil
	case <-s.stop:
		return errInternalStop
	case <-ctx.Done():
		return errInternalStop
	}
}

// runDebouncer debounces streaming events into at most the correct next action
// and gates the baton between the streaming and polling loops.
//
// Streaming messages can arrive at any time (including while a job runs) and
// in bursts (e.g. pause/resume/pause during a job); this collapses them to
// the current desired state and queues at most one job to run next.
func (s *scheduler) runDebouncer(ctx context.Context, bat *baton, outCh chan<- actionMessage, inCh <-chan actionMessage) error {
	s.logger.Debugf("[runDebouncer] Starting")
	defer s.logger.Debugf("[runDebouncer] Exiting")
	defer close(outCh)
	defer bat.Release(actorDebouncer)

	actionInProgress := false
	var lastActionResult chan error

	var pendingJobs []string
	var pendingAction string
	var pendingReason string
	pending := false

	// While healthy the debouncer holds the baton (blocking the ping loop);
	// unhealthy releases it (as soon as the current action completes).
	healthy := true

	for {
		select {
		case <-s.stop:
			return nil
		case <-ctx.Done():
			return nil

		case <-iif(healthy, bat.Acquire()):
			bat.Acquired(actorDebouncer)

		case msg, open := <-inCh:
			if !open {
				return nil
			}

			healthy = !msg.unhealthy
			if !healthy {
				// Release the baton as soon as we can: immediately if no
				// action is in progress, otherwise it stays held until the
				// stream reports healthy again (matching the main agent
				// worker's debouncer).
				if !actionInProgress {
					bat.Release(actorDebouncer)
				}
				break
			}

			switch {
			case !pending:
				// Nothing pending: the new message becomes the pending action.
				pending = true
				pendingAction = msg.action
				pendingReason = msg.reason
				if msg.jobID != "" {
					pendingJobs = append(pendingJobs, msg.jobID)
				}

			case msg.action == "disconnect":
				// Apply disconnect ASAP.
				pendingAction = "disconnect"
				pendingReason = msg.reason

			case pendingAction == "disconnect":
				// Never debounce over an existing disconnect.

			default:
				// Newest non-disconnect action replaces the pending one.
				pendingAction = msg.action
				pendingReason = msg.reason
				if msg.jobID != "" {
					pendingJobs = append(pendingJobs, msg.jobID)
				}
			}

		case <-lastActionResult:
			// The most recent action completed; fall through to send the
			// next pending action if able.
			lastActionResult = nil
			actionInProgress = false
		}

		if !healthy || !bat.HeldBy(actorDebouncer) || actionInProgress || !pending {
			continue
		}

		lastActionResult = make(chan error)
		newMsg := actionMessage{
			action: pendingAction,
			reason: pendingReason,
			errCh:  lastActionResult,
		}
		// Resumed (empty action): include the next queued job, if any.
		if pendingAction == "" && len(pendingJobs) > 0 {
			newMsg.jobID = pendingJobs[0]
		}
		select {
		case outCh <- newMsg:
			pending = false
			pendingAction = ""
			pendingReason = ""
			if newMsg.jobID != "" {
				pendingJobs = pendingJobs[1:]
				pending = len(pendingJobs) > 0
			}
			actionInProgress = true
		case <-s.stop:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

// runActionLoop serialises every action and is the single place jobs are
// delivered to the caller, guaranteeing exactly-once delivery.
//
// jobCtx (the RunDispatch context) is used for accepting and delivering jobs,
// so a graceful stop doesn't interrupt an in-flight job; loopCtx drives
// shutdown waits.
func (s *scheduler) runActionLoop(jobCtx, loopCtx context.Context, fromPingLoop, fromDebouncer <-chan actionMessage) error {
	s.logger.Debugf("[runActionLoop] Starting")
	defer s.logger.Debugf("[runActionLoop] Exiting")
	// If this loop stops, none of the others can make progress.
	defer s.internalStop()

	paused := false

	for {
		if fromPingLoop == nil && fromDebouncer == nil {
			return nil
		}

		var msg actionMessage
		select {
		case m, open := <-fromPingLoop:
			if !open {
				fromPingLoop = nil
				continue
			}
			msg = m
		case m, open := <-fromDebouncer:
			if !open {
				fromDebouncer = nil
				continue
			}
			msg = m
		case <-s.stop:
			return nil
		case <-loopCtx.Done():
			return nil
		}

		switch msg.action {
		case "disconnect":
			s.logger.Debugf("[runActionLoop] Disconnect instruction received")
			if s.handler.Disconnect != nil {
				s.handler.Disconnect(msg.reason)
			}
			// Don't respond on errCh: the streaming loop exits on
			// disconnect itself, and internalStop (deferred) stops the rest.
			return nil

		case "pause":
			s.logger.Debugf("[runActionLoop] Entering paused state")
			// Only notify on the transition into the paused state; the
			// control plane can keep repeating the pause instruction.
			if !paused {
				paused = true
				if s.handler.Pause != nil {
					s.handler.Pause(msg.reason)
				}
			}
			close(msg.errCh)
			continue
		}

		// Any other action while paused means we have resumed.
		if paused {
			s.logger.Infof("Agent has resumed after being paused")
			paused = false
			if s.handler.Resume != nil {
				s.handler.Resume()
			}
		}

		// An empty job ID is an idle message.
		if msg.jobID == "" {
			close(msg.errCh)
			continue
		}

		// Accepting and running the job is done synchronously: no further job
		// is delivered until this one converges.
		err := s.acceptAndDeliverJob(jobCtx, msg.jobID)
		if err != nil {
			s.logger.Errorf("%v", err)
			msg.errCh <- err
		}
		close(msg.errCh)
	}
}

// acceptAndDeliverJob accepts a dispatched job (retrying transient failures)
// and hands it to the caller exactly once.
func (s *scheduler) acceptAndDeliverJob(ctx context.Context, jobID string) error {
	s.logger.Infof("Assigned job %s. Accepting...", jobID)

	r := roko.NewRetrier(
		roko.WithMaxAttempts(30),
		roko.WithStrategy(roko.Constant(5*time.Second)),
		roko.WithSleepFunc(s.client.RetrySleepFunc),
	)
	accepted, err := roko.DoFunc(ctx, r, func(r *roko.Retrier) (*api.Job, error) {
		accepted, _, acceptErr := s.client.APIClient.AcceptJob(ctx, jobID)
		if acceptErr != nil {
			if api.IsRetryableError(acceptErr) {
				s.logger.Warnf("%s (%s)", acceptErr, r)
			} else {
				s.logger.Warnf("Buildkite rejected the call to accept the job (%s)", acceptErr)
				r.Break()
			}
		}
		return accepted, acceptErr
	})
	if accepted == nil {
		return fmt.Errorf("failed to accept job %q: %w", jobID, err)
	}

	return s.handler.Job(ctx, accepted)
}

// iif returns t if b is true, otherwise the zero value of T. It enables a
// select case to be switched on/off based on state evaluated at the start of
// the select.
func iif[T any](b bool, t T) T {
	if b {
		return t
	}
	var f T
	return f
}
