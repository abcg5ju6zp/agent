package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/buildkite/agent/v4/api"
	agentedgev1 "github.com/buildkite/agent/v4/api/proto/gen"
	"github.com/buildkite/agent/v4/logger"
)

// withNoDelays removes inter-ping and stream-reconnect waits.
func withNoDelays() ControllerOption {
	return func(c *controllerConfig) { c.noDelays = true }
}

func newTestController(t *testing.T, f *fakeControlPlane, scheduling bool) *Controller {
	t.Helper()
	l := logger.NewConsoleLogger(logger.NewTestPrinter(t), func(int) {})
	opts := []ControllerOption{
		WithEndpoint(f.URL + "/v3"),
		WithLogger(l),
	}
	if scheduling {
		opts = append(opts, WithScheduling(), withNoDelays())
	}
	ctrl, err := NewController(t.Context(), fakeRegToken, "embedded-agent", []string{"os=test"}, opts...)
	if err != nil {
		t.Fatalf("NewController() error = %v", err)
	}
	return ctrl
}

// reportingJob returns a DispatchHandler.Job that starts and finishes the job
// through the JobController, reports the job ID on delivered (if non-nil),
// and blocks on gate (if non-nil) between start and finish.
func reportingJob(t *testing.T, ctrl *Controller, delivered chan<- string, gate <-chan struct{}) func(context.Context, *api.Job) error {
	t.Helper()
	return func(ctx context.Context, job *api.Job) error {
		jc := ctrl.NewJobController(job)
		if err := jc.Start(ctx); err != nil {
			return err
		}
		if delivered != nil {
			select {
			case delivered <- job.ID:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if gate != nil {
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return jc.Finish(ctx, ProcessExit{Status: 0}, nil)
	}
}

// Default mode: registered as ignored from dispatches, RunDispatch rejected,
// and job-by-ID acquisition keeps working.
func TestController_DefaultMode_IgnoresInDispatches(t *testing.T) {
	t.Parallel()

	f := newFakeControlPlane(t, false)
	job := f.addJob()

	ctrl := newTestController(t, f, false)

	if f.registeredIgnore == nil || !*f.registeredIgnore {
		t.Errorf("IgnoreInDispatches registered = %v, want true", f.registeredIgnore)
	}

	if err := ctrl.RunDispatch(t.Context(), DispatchHandler{
		Job: func(context.Context, *api.Job) error { return nil },
	}); !errors.Is(err, ErrSchedulingNotEnabled) {
		t.Errorf("RunDispatch() error = %v, want ErrSchedulingNotEnabled", err)
	}

	got, err := ctrl.AcquireJob(t.Context(), job.id)
	if err != nil {
		t.Fatalf("AcquireJob() error = %v", err)
	}
	if got.ID != job.id {
		t.Errorf("AcquireJob() ID = %q, want %q", got.ID, job.id)
	}
	jc := ctrl.NewJobController(got)
	if err := jc.Start(t.Context()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := jc.Finish(t.Context(), ProcessExit{Status: 0}, nil); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	if got := f.jobState(job.id); got != "finished" {
		t.Errorf("job state = %q, want finished", got)
	}

	if err := ctrl.Close(t.Context()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !f.connected || !f.disconnected {
		t.Errorf("connected=%v disconnected=%v, want both true", f.connected, f.disconnected)
	}
}

// Polling fallback (stream unsupported): pause, resume, one job delivered
// exactly once, disconnect.
func TestDispatch_Polling_PauseResumeJobDisconnect(t *testing.T) {
	t.Parallel()

	f := newFakeControlPlane(t, false) // streaming RPC returns Unimplemented
	ctrl := newTestController(t, f, true)

	if f.registeredIgnore == nil || *f.registeredIgnore {
		t.Errorf("IgnoreInDispatches registered = %v, want false", f.registeredIgnore)
	}

	var mu sync.Mutex
	var delivered []string
	jobCh := make(chan string, 4)
	// While paused the server returns a pause action on every ping, so these
	// channels must tolerate repeated events.
	pauseCh := make(chan string, 64)
	resumeCh := make(chan struct{}, 64)
	disconnectCh := make(chan string, 64)

	h := DispatchHandler{
		Job: func(ctx context.Context, j *api.Job) error {
			err := reportingJob(t, ctrl, jobCh, nil)(ctx, j)
			mu.Lock()
			delivered = append(delivered, j.ID)
			mu.Unlock()
			return err
		},
		Pause:      func(reason string) { pauseCh <- reason },
		Resume:     func() { resumeCh <- struct{}{} },
		Disconnect: func(reason string) { disconnectCh <- reason },
	}

	done := make(chan error, 1)
	go func() { done <- ctrl.RunDispatch(t.Context(), h) }()

	waitFor(t, 5*time.Second, func() bool { return f.pings.Load() > 0 }, "polling pings to start")

	// Pause: no jobs may be delivered while paused.
	f.setPaused(true)
	select {
	case reason := <-pauseCh:
		if reason != "" {
			t.Errorf("pause reason = %q, want empty", reason)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive pause")
	}

	job := f.addJob()
	f.assign(job)
	select {
	case id := <-jobCh:
		t.Fatalf("job %q delivered while paused", id)
	case <-time.After(200 * time.Millisecond):
	}

	// Resume: the assigned job is delivered once and runs to completion.
	f.setPaused(false)
	select {
	case <-resumeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive resume")
	}
	select {
	case id := <-jobCh:
		if id != job.id {
			t.Fatalf("delivered job = %q, want %q", id, job.id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("assigned job was not delivered after resume")
	}
	waitFor(t, 5*time.Second, func() bool { return f.jobState(job.id) == "finished" }, "job to finish")

	// Exactly once: nothing else arrives.
	select {
	case id := <-jobCh:
		t.Fatalf("job %q delivered a second time", id)
	case <-time.After(100 * time.Millisecond):
	}
	mu.Lock()
	if len(delivered) != 1 || delivered[0] != job.id {
		t.Errorf("delivered = %v, want exactly [%s]", delivered, job.id)
	}
	mu.Unlock()

	// Disconnect instruction ends RunDispatch.
	f.setDisconnect()
	select {
	case <-disconnectCh:
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive disconnect")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("RunDispatch() error = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunDispatch did not return after disconnect")
	}

	if err := ctrl.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !f.disconnected {
		t.Error("agent was not disconnected")
	}
}

// Streaming dispatch: pause/resume/job/disconnect all arrive over the stream,
// with zero polling pings, using the same registered agent identity.
func TestDispatch_Streaming_PauseResumeJobDisconnect(t *testing.T) {
	t.Parallel()

	f := newFakeControlPlane(t, true)
	ctrl := newTestController(t, f, true)

	job := f.addJob()
	jobCh := make(chan string, 1)
	pauseCh := make(chan string, 1)
	resumeCh := make(chan struct{}, 1)
	disconnectCh := make(chan string, 1)

	h := DispatchHandler{
		Job:        reportingJob(t, ctrl, jobCh, nil),
		Pause:      func(reason string) { pauseCh <- reason },
		Resume:     func() { resumeCh <- struct{}{} },
		Disconnect: func(reason string) { disconnectCh <- reason },
	}

	done := make(chan error, 1)
	go func() { done <- ctrl.RunDispatch(t.Context(), h) }()

	send := func(msg *agentedgev1.StreamPingsResponse) {
		t.Helper()
		select {
		case f.streamCh <- msg:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out sending stream message")
		}
	}

	send(&agentedgev1.StreamPingsResponse{
		Action: &agentedgev1.StreamPingsResponse_Pause{
			Pause: &agentedgev1.PauseAction{Reason: "on holiday"},
		},
	})
	if reason := <-pauseCh; reason != "on holiday" {
		t.Fatalf("pause reason = %q, want %q", reason, "on holiday")
	}

	send(&agentedgev1.StreamPingsResponse{
		Action: &agentedgev1.StreamPingsResponse_Resume{},
	})
	<-resumeCh

	send(&agentedgev1.StreamPingsResponse{
		Action: &agentedgev1.StreamPingsResponse_JobAssigned{
			JobAssigned: &agentedgev1.JobAssignedAction{
				Job: &agentedgev1.Job{Id: job.id},
			},
		},
	})
	select {
	case id := <-jobCh:
		if id != job.id {
			t.Fatalf("delivered job = %q, want %q", id, job.id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("streamed job was not delivered")
	}
	waitFor(t, 5*time.Second, func() bool { return f.jobState(job.id) == "finished" }, "job to finish")

	send(&agentedgev1.StreamPingsResponse{
		Action: &agentedgev1.StreamPingsResponse_Disconnect{
			Disconnect: &agentedgev1.DisconnectAction{Reason: "go home"},
		},
	})
	if reason := <-disconnectCh; reason != "go home" {
		t.Fatalf("disconnect reason = %q, want %q", reason, "go home")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("RunDispatch() error = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunDispatch did not return after stream disconnect")
	}

	if got := f.pings.Load(); got != 0 {
		t.Errorf("polling pings = %d, want 0 while stream healthy", got)
	}
	waitFor(t, 5*time.Second, func() bool { return f.streamConns.Load() >= 1 }, "a stream connection")
	f.mu.Lock()
	for _, id := range f.streamAgentIDs {
		if id != ctrl.agent.UUID {
			t.Errorf("stream used agent ID %q, want %q", id, ctrl.agent.UUID)
		}
	}
	f.mu.Unlock()

	if err := ctrl.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// A recoverable stream error hands dispatch to polling (the job is delivered
// there); a successful reconnect then restores streaming for the same agent,
// and a later disconnect arrives over the restored stream.
func TestDispatch_StreamInterruption_FallbackAndRestore(t *testing.T) {
	t.Parallel()

	f := newFakeControlPlane(t, true)
	ctrl := newTestController(t, f, true)

	job := f.addJob()
	f.assign(job)
	jobCh := make(chan string, 1)
	conn2Disconnect := make(chan struct{})

	f.streamHandler = func(
		ctx context.Context,
		req *connect.Request[agentedgev1.StreamPingsRequest],
		resp *connect.ServerStream[agentedgev1.StreamPingsResponse],
	) error {
		switch n := f.streamConns.Load(); n {
		case 1:
			// Transient failure: stream goes unhealthy, polling takes over.
			return connect.NewError(connect.CodeUnavailable, errors.New("temporary outage"))

		case 2:
			// Don't reclaim dispatch until polling has demonstrably run.
			waitFor(t, 5*time.Second, func() bool { return f.pings.Load() > 0 }, "polling fallback pings")
			if err := resp.Send(&agentedgev1.StreamPingsResponse{
				Action: &agentedgev1.StreamPingsResponse_Resume{},
			}); err != nil {
				return err
			}
			select {
			case <-conn2Disconnect:
				return resp.Send(&agentedgev1.StreamPingsResponse{
					Action: &agentedgev1.StreamPingsResponse_Disconnect{},
				})
			case <-ctx.Done():
				return nil
			}

		default:
			return connect.NewError(connect.CodeInternal, errors.New("unexpected stream connection"))
		}
	}

	h := DispatchHandler{Job: reportingJob(t, ctrl, jobCh, nil)}
	done := make(chan error, 1)
	go func() { done <- ctrl.RunDispatch(t.Context(), h) }()

	// The job is delivered via the polling fallback.
	select {
	case id := <-jobCh:
		if id != job.id {
			t.Fatalf("delivered job = %q, want %q", id, job.id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job was not delivered via polling fallback")
	}
	waitFor(t, 5*time.Second, func() bool { return f.jobState(job.id) == "finished" }, "job to finish")

	// Reconnect happened with the same agent identity.
	waitFor(t, 5*time.Second, func() bool { return f.streamConns.Load() >= 2 }, "stream reconnect")
	f.mu.Lock()
	for _, id := range f.streamAgentIDs {
		if id != ctrl.agent.UUID {
			t.Errorf("stream used agent ID %q, want %q", id, ctrl.agent.UUID)
		}
	}
	f.mu.Unlock()

	// Disconnect is delivered over the restored stream (proving streaming
	// took back over from polling).
	close(conn2Disconnect)
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("RunDispatch() error = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunDispatch did not return after stream disconnect")
	}

	if err := ctrl.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// Graceful shutdown stops accepting new jobs, waits for the job already
// delivered to converge (start -> finish), and only then disconnects.
func TestDispatch_GracefulClose_WaitsForInFlightJob(t *testing.T) {
	t.Parallel()

	f := newFakeControlPlane(t, false)
	ctrl := newTestController(t, f, true)

	job := f.addJob()
	f.assign(job)

	gate := make(chan struct{})
	started := make(chan string, 1)
	h := DispatchHandler{Job: reportingJob(t, ctrl, started, gate)}

	go func() { _ = ctrl.RunDispatch(t.Context(), h) }()

	select {
	case id := <-started:
		if id != job.id {
			t.Fatalf("started job = %q, want %q", id, job.id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job did not start")
	}
	waitFor(t, 5*time.Second, func() bool { return f.jobState(job.id) == "running" }, "job running")

	// Close must block while the delivered job is still running.
	closeReturned := make(chan error, 1)
	go func() { closeReturned <- ctrl.Close(context.Background()) }()
	select {
	case err := <-closeReturned:
		t.Fatalf("Close returned while job in flight (err=%v)", err)
	case <-time.After(200 * time.Millisecond):
	}
	if f.disconnected {
		t.Error("agent disconnected before the in-flight job converged")
	}

	// Let the job finish; Close then completes and disconnects.
	close(gate)
	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the job finished")
	}
	if got := f.jobState(job.id); got != "finished" {
		t.Errorf("job state = %q, want finished", got)
	}
	if !f.disconnected {
		t.Error("agent was not disconnected")
	}
}

// Jobs received from the stream after shutdown starts are not delivered; only
// the single job already in flight converges, and it is delivered exactly once.
func TestDispatch_Close_PendingJobNotDelivered_ExactlyOnce(t *testing.T) {
	t.Parallel()

	f := newFakeControlPlane(t, true)
	ctrl := newTestController(t, f, true)

	job1 := f.addJob()
	job2 := f.addJob()

	gate := make(chan struct{})
	delivered := make(chan string, 4)
	h := DispatchHandler{Job: reportingJob(t, ctrl, delivered, gate)}

	go func() { _ = ctrl.RunDispatch(t.Context(), h) }()

	send := func(msg *agentedgev1.StreamPingsResponse) {
		t.Helper()
		select {
		case f.streamCh <- msg:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out sending stream message")
		}
	}
	jobMsg := func(id string) *agentedgev1.StreamPingsResponse {
		return &agentedgev1.StreamPingsResponse{
			Action: &agentedgev1.StreamPingsResponse_JobAssigned{
				JobAssigned: &agentedgev1.JobAssignedAction{Job: &agentedgev1.Job{Id: id}},
			},
		}
	}

	send(jobMsg(job1.id))
	select {
	case id := <-delivered:
		if id != job1.id {
			t.Fatalf("first delivery = %q, want %q", id, job1.id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first job was not delivered")
	}

	// job2 arrives while job1 is in flight and gets queued by the debouncer.
	send(jobMsg(job2.id))

	closeReturned := make(chan error, 1)
	go func() { closeReturned <- ctrl.Close(context.Background()) }()
	// Give Close a moment to begin shutdown, then let job1 converge.
	time.Sleep(100 * time.Millisecond)
	close(gate)

	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}

	// job1 delivered exactly once; job2 never accepted or delivered.
	select {
	case id := <-delivered:
		t.Fatalf("unexpected delivery after shutdown: %s", id)
	case <-time.After(100 * time.Millisecond):
	}
	if got := f.jobState(job1.id); got != "finished" {
		t.Errorf("job1 state = %q, want finished", got)
	}
	if got := f.jobState(job2.id); got != "scheduled" {
		t.Errorf("job2 state = %q, want scheduled (never accepted)", got)
	}
}

