package core

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/buildkite/agent/v4/api"
	"github.com/buildkite/agent/v4/logger"
)

// fakeAPI is a controllable APIClient for tests. It records the requests it
// receives and delegates outcomes to per-method hooks.
type fakeAPI struct {
	mu sync.Mutex

	startCalls  []string // StartedAt values, in arrival order
	finishCalls []fakeFinishCall
	chunkCalls  []api.Chunk
	eventLog    []string // "start", "finish", "chunk:<seq>" in arrival order

	onStart  func(*api.Job) (*api.Response, error)
	onFinish func(*api.Job, *bool) (*api.Response, error)
	onChunk  func(string, *api.Chunk) (*api.Response, error)
}

type fakeFinishCall struct {
	FinishedAt         string
	ExitStatus         string
	Signal             string
	SignalReason       string
	ChunksFailedCount  int
	IgnoreInDispatches *bool
}

func (f *fakeAPI) AcquireJob(context.Context, string, ...api.Header) (*api.Job, *api.Response, error) {
	return nil, nil, nil
}

func (f *fakeAPI) Connect(context.Context) (*api.Response, error) { return f.resp200(), nil }

func (f *fakeAPI) Disconnect(context.Context) (*api.Response, error) { return f.resp200(), nil }

func (f *fakeAPI) Register(context.Context, *api.AgentRegisterRequest) (*api.AgentRegisterResponse, *api.Response, error) {
	return nil, nil, nil
}

func (f *fakeAPI) StartJob(_ context.Context, job *api.Job) (*api.Response, error) {
	f.mu.Lock()
	f.startCalls = append(f.startCalls, job.StartedAt)
	f.eventLog = append(f.eventLog, "start")
	hook := f.onStart
	f.mu.Unlock()
	if hook != nil {
		return hook(job)
	}
	return f.resp200(), nil
}

func (f *fakeAPI) FinishJob(_ context.Context, job *api.Job, ignore *bool) (*api.Response, error) {
	f.mu.Lock()
	f.finishCalls = append(f.finishCalls, fakeFinishCall{
		FinishedAt:         job.FinishedAt,
		ExitStatus:         job.ExitStatus,
		Signal:             job.Signal,
		SignalReason:       job.SignalReason,
		ChunksFailedCount:  job.ChunksFailedCount,
		IgnoreInDispatches: ignore,
	})
	f.eventLog = append(f.eventLog, "finish")
	hook := f.onFinish
	f.mu.Unlock()
	if hook != nil {
		return hook(job, ignore)
	}
	return f.resp200(), nil
}

func (f *fakeAPI) UploadChunk(_ context.Context, jobID string, chunk *api.Chunk) (*api.Response, error) {
	f.mu.Lock()
	record := api.Chunk{
		Data:     append([]byte(nil), chunk.Data...),
		Sequence: chunk.Sequence,
		Offset:   chunk.Offset,
		Size:     chunk.Size,
	}
	f.chunkCalls = append(f.chunkCalls, record)
	f.eventLog = append(f.eventLog, fmt.Sprintf("chunk:%d", chunk.Sequence))
	hook := f.onChunk
	f.mu.Unlock()
	if hook != nil {
		return hook(jobID, chunk)
	}
	return f.resp200(), nil
}

func apiResp(status int) *api.Response {
	return &api.Response{Response: &http.Response{StatusCode: status}}
}

func (f *fakeAPI) resp(status int) *api.Response {
	return apiResp(status)
}

func (f *fakeAPI) resp200() *api.Response { return f.resp(200) }

func (f *fakeAPI) snapshot() ([]string, []fakeFinishCall, []api.Chunk, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	starts := append([]string(nil), f.startCalls...)
	finishes := append([]fakeFinishCall(nil), f.finishCalls...)
	chunks := append([]api.Chunk(nil), f.chunkCalls...)
	events := append([]string(nil), f.eventLog...)
	return starts, finishes, chunks, events
}

func newTestClient(t *testing.T, fake *fakeAPI) *Client {
	t.Helper()
	return &Client{
		APIClient:      fake,
		Logger:         logger.Discard,
		RetrySleepFunc: func(time.Duration) {},
	}
}

func newTestJobController(t *testing.T, client *Client, jobID string) *JobController {
	t.Helper()
	jc, err := NewJobControllerWithOptions(client, &api.Job{ID: jobID},
		WithStateDir(t.TempDir()),
		WithStaleLockTimeout(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewJobControllerWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = jc.Close() })
	return jc
}

func assertRFC3339NanoUTC(t *testing.T, value, what string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("%s %q is not RFC3339Nano: %v", what, value, err)
	}
	if parsed.Location() != time.UTC {
		t.Fatalf("%s %q is not UTC", what, value)
	}
	if parsed.Format(time.RFC3339Nano) != value {
		t.Fatalf("%s %q does not round-trip (got %q)", what, value, parsed.Format(time.RFC3339Nano))
	}
	return parsed
}

// A normal run with no failures must look exactly like the old
// Start/WriteLog/Finish flow: sequence starts at 0, offsets accumulate, one
// start and one finish are sent, timestamps are RFC3339Nano UTC.
func TestJobControllerNormalFlowCompatibility(t *testing.T) {
	t.Parallel()
	fake := &fakeAPI{}
	client := newTestClient(t, fake)
	jc := newTestJobController(t, client, "job-normal")
	ctx := t.Context()

	if err := jc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := jc.WriteLog(ctx, "a"); err != nil {
		t.Fatalf("WriteLog a: %v", err)
	}
	if err := jc.WriteLog(ctx, ""); err != nil { // empty chunks behave like before
		t.Fatalf("WriteLog empty: %v", err)
	}
	if err := jc.WriteLog(ctx, "bb"); err != nil {
		t.Fatalf("WriteLog bb: %v", err)
	}
	exit := ProcessExit{Status: 7}
	if err := jc.Finish(ctx, exit, nil); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	starts, finishes, chunks, events := fake.snapshot()
	if len(starts) != 1 {
		t.Fatalf("want 1 start call, got %d", len(starts))
	}
	assertRFC3339NanoUTC(t, starts[0], "started_at")
	if len(finishes) != 1 {
		t.Fatalf("want 1 finish call, got %d", len(finishes))
	}
	assertRFC3339NanoUTC(t, finishes[0].FinishedAt, "finished_at")
	if finishes[0].ExitStatus != "7" || finishes[0].Signal != "" || finishes[0].SignalReason != "" {
		t.Fatalf("unexpected finish payload: %+v", finishes[0])
	}
	if finishes[0].ChunksFailedCount != 0 {
		t.Fatalf("want chunks_failed_count 0, got %d", finishes[0].ChunksFailedCount)
	}

	wantChunks := []api.Chunk{
		{Data: []byte("a"), Sequence: 0, Offset: 0, Size: 1},
		{Data: []byte(""), Sequence: 1, Offset: 1, Size: 0},
		{Data: []byte("bb"), Sequence: 2, Offset: 1, Size: 2},
	}
	if len(chunks) != len(wantChunks) {
		t.Fatalf("want %d chunk calls, got %d: %+v", len(wantChunks), len(chunks), chunks)
	}
	for i, want := range wantChunks {
		got := chunks[i]
		if got.Sequence != want.Sequence || got.Offset != want.Offset || got.Size != want.Size || string(got.Data) != string(want.Data) {
			t.Fatalf("chunk %d mismatch: got %+v, want %+v", i, got, want)
		}
	}
	// All chunk events precede the finish event.
	for i, ev := range events {
		if ev == "finish" {
			if i != len(events)-1 {
				t.Fatalf("finish was submitted before all chunks: %v", events)
			}
			break
		}
	}

	// Repeated Start/Finish after acknowledgement converge without new calls.
	if err := jc.Start(ctx); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if err := jc.Finish(ctx, exit, nil); err != nil {
		t.Fatalf("second Finish: %v", err)
	}
	starts2, finishes2, _, _ := fake.snapshot()
	if len(starts2) != 1 || len(finishes2) != 1 {
		t.Fatalf("idempotent Start/Finish made extra calls: starts=%d finishes=%d", len(starts2), len(finishes2))
	}

	// Writes after an acknowledged finish are rejected.
	if err := jc.WriteLog(ctx, "late"); !errors.Is(err, ErrJobFinished) {
		t.Fatalf("want ErrJobFinished, got %v", err)
	}
}

// An uncertain chunk response is retried as the same chunk (same sequence,
// offset, and data) rather than generating a new one.
func TestWriteLogRetriesSameChunkOnUncertainResponse(t *testing.T) {
	t.Parallel()
	fake := &fakeAPI{}
	var attempts int
	fake.onChunk = func(_ string, chunk *api.Chunk) (*api.Response, error) {
		attempts++
		if attempts == 1 {
			return nil, errors.New("connection reset by peer") // no response: outcome unknown
		}
		return apiResp(200), nil
	}
	client := newTestClient(t, fake)
	jc := newTestJobController(t, client, "job-retry")

	if err := jc.WriteLog(t.Context(), "hello"); err != nil {
		t.Fatalf("WriteLog: %v", err)
	}
	_, _, chunks, _ := fake.snapshot()
	if len(chunks) != 2 {
		t.Fatalf("want 2 chunk attempts, got %d", len(chunks))
	}
	for i, ch := range chunks {
		if ch.Sequence != 0 || ch.Offset != 0 || ch.Size != 5 || string(ch.Data) != "hello" {
			t.Fatalf("attempt %d is not the same chunk: %+v", i, ch)
		}
	}
}

// A chunk that is enqueued but unacknowledged when the process exits is
// persisted; the restarted controller re-delivers the same chunk and only then
// submits the persisted finish once, with the original timestamps.
func TestResumeAfterCrashMidChunkAndFinish(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fake := &fakeAPI{
		onChunk: func(string, *api.Chunk) (*api.Response, error) {
			return nil, errors.New("connection lost") // exhausts the in-call retries
		},
	}
	client := newTestClient(t, fake)
	job := &api.Job{ID: "job-crash"}

	jc1, err := NewJobControllerWithOptions(client, job,
		WithStateDir(dir), WithStaleLockTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatalf("NewJobControllerWithOptions: %v", err)
	}
	ctx := t.Context()
	if err := jc1.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := jc1.WriteLog(ctx, "orphaned"); err == nil {
		t.Fatal("want WriteLog error while API unreachable")
	}
	cancelExit := ProcessExit{Status: 1, Signal: "TERM", SignalReason: SignalReasonCancel}
	if err := jc1.Finish(ctx, cancelExit, nil); err == nil {
		t.Fatal("want Finish error while chunk unacknowledged")
	}
	_, finishesBefore, _, eventsBefore := fake.snapshot()
	if len(finishesBefore) != 0 {
		t.Fatalf("finish must not be submitted with pending logs: %v", eventsBefore)
	}
	if err := jc1.Close(); err != nil { // "process exits"
		t.Fatalf("Close: %v", err)
	}

	// Restart: the API is reachable again.
	fake.onChunk = nil
	jc2, err := NewJobControllerWithOptions(client, job,
		WithStateDir(dir), WithStaleLockTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatalf("NewJobControllerWithOptions after restart: %v", err)
	}
	t.Cleanup(func() { _ = jc2.Close() })

	if err := jc2.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	starts, finishes, chunks, events := fake.snapshot()
	if len(starts) != 1 {
		t.Fatalf("an acknowledged start must not be re-sent, got %d starts", len(starts))
	}
	if len(chunks) == 0 {
		t.Fatal("the orphaned chunk was not re-delivered")
	}
	last := chunks[len(chunks)-1]
	if last.Sequence != 0 || last.Offset != 0 || string(last.Data) != "orphaned" {
		t.Fatalf("orphaned chunk not re-sent as the same chunk: %+v", last)
	}
	if len(finishes) != 1 {
		t.Fatalf("want exactly 1 finish, got %d", len(finishes))
	}
	fc := finishes[0]
	if fc.ExitStatus != "1" || fc.Signal != "TERM" || fc.SignalReason != SignalReasonCancel {
		t.Fatalf("persisted final result not preserved: %+v", fc)
	}
	assertRFC3339NanoUTC(t, fc.FinishedAt, "finished_at")
	if events[len(events)-1] != "finish" {
		t.Fatalf("finish was not the last action: %v", events)
	}

	// Resuming again converges with no new remote calls.
	if err := jc2.Resume(ctx); err != nil {
		t.Fatalf("second Resume: %v", err)
	}
	_, finishes2, chunks2, _ := fake.snapshot()
	if len(finishes2) != 1 || len(chunks2) != len(chunks) {
		t.Fatalf("second Resume made extra calls: chunks=%d (was %d), finishes=%d", len(chunks2), len(chunks), len(finishes2))
	}
}

// A start whose response is lost is re-sent after restart with the original
// StartedAt timestamp, not a fresh one.
func TestStartResponseLostKeepsTimestamp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fake := &fakeAPI{
		onStart: func(*api.Job) (*api.Response, error) {
			return nil, errors.New("connection lost")
		},
	}
	client := newTestClient(t, fake)
	job := &api.Job{ID: "job-start-lost"}

	jc1, err := NewJobControllerWithOptions(client, job,
		WithStateDir(dir), WithStaleLockTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatalf("NewJobControllerWithOptions: %v", err)
	}
	ctx := t.Context()
	if err := jc1.Start(ctx); err == nil {
		t.Fatal("want Start error while API unreachable")
	}
	starts, _, _, _ := fake.snapshot()
	if len(starts) == 0 {
		t.Fatal("expected start attempts")
	}
	original := starts[0]
	if err := jc1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fake.onStart = nil
	jc2, err := NewJobControllerWithOptions(client, job,
		WithStateDir(dir), WithStaleLockTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatalf("NewJobControllerWithOptions after restart: %v", err)
	}
	t.Cleanup(func() { _ = jc2.Close() })
	if err := jc2.Start(ctx); err != nil {
		t.Fatalf("Start after restart: %v", err)
	}
	starts2, _, _, _ := fake.snapshot()
	if len(starts2) < 2 {
		t.Fatalf("want the start to be re-sent, got %d attempts", len(starts2))
	}
	if starts2[len(starts2)-1] != original {
		t.Fatalf("restart re-sent start with a new timestamp: %q != %q", starts2[len(starts2)-1], original)
	}

	// A third Start converges without contacting the API.
	if err := jc2.Start(ctx); err != nil {
		t.Fatalf("third Start: %v", err)
	}
	starts3, _, _, _ := fake.snapshot()
	if len(starts3) != len(starts2) {
		t.Fatal("acknowledged Start made another API call")
	}
}

// A finish the server already accepted but whose response was lost converges
// on a 422 when re-sent from persisted state.
func TestFinish422AfterLostResponseConverges(t *testing.T) {
	t.Parallel()
	fake := &fakeAPI{
		onFinish: func(*api.Job, *bool) (*api.Response, error) {
			return apiResp(422), errors.New("422 Unprocessable Entity")
		},
	}
	client := newTestClient(t, fake)
	jc := newTestJobController(t, client, "job-finish-422")
	ctx := t.Context()
	if err := jc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// A 422 on the FIRST finish attempt remains an error (same as old
	// behaviour: Buildkite rejected the transition).
	if err := jc.Finish(ctx, ProcessExit{Status: 1}, nil); err == nil {
		t.Fatal("want error on first finish attempt that gets 422")
	}

	// Simulate the original request actually having reached the server, then
	// a restart: Resume re-sends and treats 422 as convergence.
	if err := jc.Resume(ctx); err != nil {
		t.Fatalf("Resume should converge on 422 for a prior finish, got %v", err)
	}
	state := jc.session.state
	if !state.Finish.Acked {
		t.Fatal("finish not marked acknowledged after convergence")
	}
}

// Finish must not be submitted while any log chunk is unacknowledged.
func TestFinishBlockedByPendingChunk(t *testing.T) {
	t.Parallel()
	fake := &fakeAPI{
		onChunk: func(string, *api.Chunk) (*api.Response, error) {
			// A 4xx breaks the in-call retries immediately.
			return apiResp(400), errors.New("400 Bad Request")
		},
	}
	client := newTestClient(t, fake)
	jc := newTestJobController(t, client, "job-pending")
	ctx := t.Context()
	if err := jc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := jc.WriteLog(ctx, "stuck"); err == nil {
		t.Fatal("want WriteLog error")
	}
	if err := jc.Finish(ctx, ProcessExit{Status: 1}, nil); err == nil {
		t.Fatal("want Finish error while a chunk is pending")
	}
	_, finishes, _, events := fake.snapshot()
	if len(finishes) != 0 {
		t.Fatalf("finish was submitted with a pending chunk: %v", events)
	}

	// Once the chunk can be delivered, Resume drains it and then finishes.
	fake.onChunk = nil
	if err := jc.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	_, finishes2, chunks, events2 := fake.snapshot()
	if len(finishes2) != 1 {
		t.Fatalf("want 1 finish after convergence, got %d", len(finishes2))
	}
	fi := -1
	ci := -1
	for i, ev := range events2 {
		switch ev {
		case "chunk:0":
			ci = i
		case "finish":
			fi = i
		}
	}
	if ci < 0 || fi < 0 || ci > fi {
		t.Fatalf("chunk not delivered before finish: %v", events2)
	}
	if chunks[len(chunks)-1].Sequence != 0 || string(chunks[len(chunks)-1].Data) != "stuck" {
		t.Fatalf("pending chunk re-sent incorrectly: %+v", chunks[len(chunks)-1])
	}
}

// Concurrent writers are delivered in one deterministic, gap-free order.
func TestConcurrentWritersDeterministicOrder(t *testing.T) {
	t.Parallel()
	fake := &fakeAPI{}
	client := newTestClient(t, fake)
	jc := newTestJobController(t, client, "job-concurrent")
	ctx := t.Context()
	if err := jc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const n = 25
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := jc.WriteLog(ctx, fmt.Sprintf("%04d", i)); err != nil {
				t.Errorf("WriteLog %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	_, _, chunks, _ := fake.snapshot()
	if len(chunks) != n {
		t.Fatalf("want %d chunks, got %d", n, len(chunks))
	}
	seenData := make(map[string]bool, n)
	for i, ch := range chunks {
		if ch.Sequence != uint64(i) {
			t.Fatalf("chunks not delivered in gap-free sequence order at %d: got seq %d", i, ch.Sequence)
		}
		if ch.Offset != uint64(i)*4 || ch.Size != 4 {
			t.Fatalf("chunk %d has wrong offset/size: offset=%d size=%d", i, ch.Offset, ch.Size)
		}
		if len(ch.Data) != 4 {
			t.Fatalf("chunk %d has wrong data: %q", i, ch.Data)
		}
		seenData[string(ch.Data)] = true
	}
	if len(seenData) != n {
		t.Fatalf("want %d distinct chunk payloads, got %d", n, len(seenData))
	}
}

// Chunks persisted from a previous process are delivered before any newly
// enqueued chunk, and sequence numbering continues rather than restarting.
func TestRestartDeliversPendingBeforeNewChunks(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fake := &fakeAPI{
		onChunk: func(string, *api.Chunk) (*api.Response, error) {
			return apiResp(400), errors.New("400 Bad Request")
		},
	}
	client := newTestClient(t, fake)
	job := &api.Job{ID: "job-merge"}

	jc1, err := NewJobControllerWithOptions(client, job, WithStateDir(dir))
	if err != nil {
		t.Fatalf("NewJobControllerWithOptions: %v", err)
	}
	if err := jc1.WriteLog(t.Context(), "old"); err == nil {
		t.Fatal("want WriteLog error")
	}
	if err := jc1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fake.onChunk = nil
	jc2, err := NewJobControllerWithOptions(client, job, WithStateDir(dir))
	if err != nil {
		t.Fatalf("NewJobControllerWithOptions after restart: %v", err)
	}
	t.Cleanup(func() { _ = jc2.Close() })

	_, _, chunksBefore, _ := fake.snapshot()
	baseline := len(chunksBefore)
	if err := jc2.WriteLog(t.Context(), "new"); err != nil {
		t.Fatalf("WriteLog after restart: %v", err)
	}

	_, _, chunks, events := fake.snapshot()
	delivered := chunks[baseline:]
	if len(delivered) != 2 {
		t.Fatalf("want 2 chunk deliveries after restart, got %d (events %v)", len(delivered), events)
	}
	if string(delivered[0].Data) != "old" || delivered[0].Sequence != 0 || delivered[0].Offset != 0 {
		t.Fatalf("pending chunk must be delivered first: %+v", delivered[0])
	}
	if string(delivered[1].Data) != "new" || delivered[1].Sequence != 1 || delivered[1].Offset != 3 {
		t.Fatalf("new chunk must continue the sequence: %+v", delivered[1])
	}
	postEvents := events[len(events)-2:]
	if postEvents[0] != "chunk:0" || postEvents[1] != "chunk:1" {
		t.Fatalf("delivery order not deterministic: %v", events)
	}
}

// A newer session fences a still-live ("stale") controller: the stale
// controller's operations fail and never overwrite the newer session's state,
// while the newer session recovers the old chunks and continues numbering.
func TestFencingStaleSessionCannotOverwrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fake := &fakeAPI{}
	client := newTestClient(t, fake)
	job := &api.Job{ID: "job-fence"}
	ctx := t.Context()

	jc1, err := NewJobControllerWithOptions(client, job,
		WithStateDir(dir), WithStaleLockTimeout(40*time.Millisecond))
	if err != nil {
		t.Fatalf("NewJobControllerWithOptions jc1: %v", err)
	}
	if err := jc1.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := jc1.WriteLog(ctx, "first"); err != nil {
		t.Fatalf("WriteLog first: %v", err)
	}
	if jc1.session.state.Epoch != 1 {
		t.Fatalf("want initial epoch 1, got %d", jc1.session.state.Epoch)
	}

	// A new controller takes over while jc1 is still alive and holds the lock.
	jc2, err := NewJobControllerWithOptions(client, job,
		WithStateDir(dir), WithStaleLockTimeout(40*time.Millisecond))
	if err != nil {
		t.Fatalf("NewJobControllerWithOptions jc2: %v", err)
	}
	t.Cleanup(func() { _ = jc2.Close() })
	if jc2.session.state.Epoch != 2 {
		t.Fatalf("want takeover epoch 2, got %d", jc2.session.state.Epoch)
	}

	// The stale controller cannot enqueue, resume, or finish.
	if err := jc1.WriteLog(ctx, "stale"); !errors.Is(err, ErrSessionSuperseded) {
		t.Fatalf("stale WriteLog want ErrSessionSuperseded, got %v", err)
	}
	if err := jc1.Resume(ctx); !errors.Is(err, ErrSessionSuperseded) {
		t.Fatalf("stale Resume want ErrSessionSuperseded, got %v", err)
	}
	if err := jc1.Finish(ctx, ProcessExit{Status: 1}, nil); !errors.Is(err, ErrSessionSuperseded) {
		t.Fatalf("stale Finish want ErrSessionSuperseded, got %v", err)
	}

	// The stale append never reached the log, and the new session stays epoch 2.
	records, err := readWAL(jc2.session.walAt)
	if err != nil {
		t.Fatalf("readWAL: %v", err)
	}
	if len(records) != 1 || string(records[0].Data) != "first" {
		t.Fatalf("stale session mutated the chunk log: %+v", records)
	}
	disk, existed, err := jc2.session.loadState()
	if err != nil || !existed || disk.Epoch != 2 {
		t.Fatalf("new session state damaged: existed=%v epoch=%d err=%v", existed, disk.Epoch, err)
	}

	// While the stale owner holds the lock, the new session can recover but
	// cannot assign new sequences.
	if err := jc2.WriteLog(ctx, "locked"); !errors.Is(err, ErrSessionLocked) {
		t.Fatalf("locked-out WriteLog want ErrSessionLocked, got %v", err)
	}

	// Stale process exits; the new session acquires the lock and continues.
	if err := jc1.Close(); err != nil {
		t.Fatalf("jc1 Close: %v", err)
	}
	if err := jc2.WriteLog(ctx, "second"); err != nil {
		t.Fatalf("WriteLog second after takeover: %v", err)
	}
	records, err = readWAL(jc2.session.walAt)
	if err != nil {
		t.Fatalf("readWAL: %v", err)
	}
	if len(records) != 2 || string(records[0].Data) != "first" || string(records[1].Data) != "second" {
		t.Fatalf("unexpected chunk log after takeover: %+v", records)
	}
	if records[1].Sequence != 1 || records[1].Offset != 5 {
		t.Fatalf("new chunk must continue at sequence 1: %+v", records[1])
	}

	if err := jc2.Finish(ctx, ProcessExit{Status: 0}, nil); err != nil {
		t.Fatalf("Finish after takeover: %v", err)
	}
	_, finishes, chunks, _ := fake.snapshot()
	if len(finishes) != 1 {
		t.Fatalf("want exactly 1 finish, got %d", len(finishes))
	}
	if len(chunks) != 2 {
		t.Fatalf("stale chunk was delivered: %+v", chunks)
	}
}

// The write-ahead log contains the chunk record while its upload is in flight,
// and the acknowledged prefix only advances after acknowledgement.
func TestChunkPersistedBeforeAndAfterAcknowledgement(t *testing.T) {
	t.Parallel()
	fake := &fakeAPI{}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	fake.onChunk = func(string, *api.Chunk) (*api.Response, error) {
		entered <- struct{}{}
		<-release
		return fake.resp200(), nil
	}
	client := newTestClient(t, fake)
	jc := newTestJobController(t, client, "job-durable")
	ctx := t.Context()
	if err := jc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- jc.WriteLog(ctx, "inflight") }()
	<-entered

	records, err := readWAL(jc.session.walAt)
	if err != nil {
		t.Fatalf("readWAL during flight: %v", err)
	}
	if len(records) != 1 || string(records[0].Data) != "inflight" || records[0].Sequence != 0 || records[0].Offset != 0 {
		t.Fatalf("chunk not persisted before upload completed: %+v", records)
	}
	disk, existed, err := jc.session.loadState()
	if err != nil || !existed {
		t.Fatalf("state missing during flight: %v", err)
	}
	if disk.AckedSequence != 0 || disk.NextSequence != 1 {
		t.Fatalf("unexpected counters during flight: acked=%d next=%d", disk.AckedSequence, disk.NextSequence)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("WriteLog: %v", err)
	}
	disk, _, err = jc.session.loadState()
	if err != nil {
		t.Fatalf("loadState after ack: %v", err)
	}
	if disk.AckedSequence != 1 || disk.AckedOffset != 8 {
		t.Fatalf("acknowledged prefix not advanced: ackedSeq=%d ackedOff=%d", disk.AckedSequence, disk.AckedOffset)
	}
}

// BKTimestamp keeps its existing millisecond header format.
func TestBKTimestampFormat(t *testing.T) {
	t.Parallel()
	ts := time.Unix(123, 456*int64(time.Millisecond))
	got := BKTimestamp(ts)
	want := fmt.Sprintf("\x1b_bk;t=%d\x07", ts.UnixNano()/int64(time.Millisecond))
	if got != want {
		t.Fatalf("BKTimestamp format changed: %q want %q", got, want)
	}
}
