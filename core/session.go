package core

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildkite/agent/v4/api"
	"github.com/buildkite/agent/v4/logger"
	"github.com/buildkite/roko"
	"github.com/gofrs/flock"
)

// SignalReasonCancel is the signal reason recorded for a job that ends because
// it was cancelled (for example from the Buildkite dashboard or API). It is
// persisted as part of the recoverable finish, so a crash, forced exit, or a
// lost response while finishing a cancelled job still converges after restart.
const SignalReasonCancel = "cancel"

var (
	// ErrSessionSuperseded is returned when a controller's delivery session
	// has been taken over by a newer controller (for example when a new
	// controller was started after a restart while this process was still
	// alive). A superseded controller never touches the session state again,
	// so a stale process cannot overwrite the newer session.
	ErrSessionSuperseded = errors.New("job delivery session was superseded by a newer controller")

	// ErrJobFinished is returned when a log write is attempted after the
	// job's finish has been acknowledged.
	ErrJobFinished = errors.New("job has already finished")

	// ErrSessionLocked is returned when a new log chunk cannot durably be
	// enqueued because another live process still holds the session lock and
	// has not yet been fenced out. Recovery (resending chunks already
	// enqueued, and finishing the job) remains possible while locked out.
	ErrSessionLocked = errors.New("job delivery session is locked by another live process")
)

const (
	sessionSchemaVersion = 1

	// defaultStaleLockTimeout bounds how long a new controller waits for a
	// live session owner to release its lock before fencing the owner and
	// continuing recovery without the lock.
	defaultStaleLockTimeout = 10 * time.Second

	// walHeaderSize is the fixed size of a write-ahead log record header:
	// sequence (8) + offset (8) + size (8).
	walHeaderSize = 24
)

var walTempCounter atomic.Uint64

// pendingChunkState is one log chunk whose delivery has not yet been
// acknowledged by Buildkite.
type pendingChunkState struct {
	Sequence uint64 `json:"sequence"`
	Offset   uint64 `json:"offset"`
	Size     uint64 `json:"size"`
	Data     []byte `json:"data"`
}

// finishSessionState is the durable final-result ("finish") state of a job.
type finishSessionState struct {
	// Requested is true once Finish has been called and the final result has
	// been durably recorded.
	Requested bool `json:"requested"`

	// Acked is true once the final result has been accepted by Buildkite.
	Acked bool `json:"acked"`

	FinishedAt              string `json:"finished_at,omitempty"`
	ExitStatus              int    `json:"exit_status"`
	Signal                  string `json:"signal,omitempty"`
	SignalReason            string `json:"signal_reason,omitempty"`
	IgnoreAgentInDispatches *bool  `json:"ignore_agent_in_dispatches,omitempty"`
}

// jobSessionState is the on-disk, recoverable state of one embedded job's
// delivery session.
type jobSessionState struct {
	SchemaVersion int    `json:"schema_version"`
	JobID         string `json:"job_id"`

	// Epoch is the fencing generation. It is bumped every time a controller
	// takes the session over. Commits from a controller with an older epoch
	// are rejected.
	Epoch uint64 `json:"epoch"`

	// Job start state. StartedAt keeps the same RFC3339Nano UTC format used by
	// the API client.
	Started    bool   `json:"started"`
	StartAcked bool   `json:"start_acked"`
	StartedAt  string `json:"started_at,omitempty"`

	// NextSequence and NextOffset mirror the tail of the chunk write-ahead
	// log (the values to assign to the next newly enqueued chunk). They are
	// reconciled from the log on takeover, so a crash between appending a
	// record and updating this state still recovers correctly.
	NextSequence uint64 `json:"next_sequence"`
	NextOffset   uint64 `json:"next_offset"`

	// AckedSequence is one past the highest contiguously acknowledged chunk
	// sequence; AckedOffset is the corresponding byte offset. Chunks in the
	// write-ahead log with a sequence >= AckedSequence are pending delivery.
	AckedSequence uint64 `json:"acked_sequence"`
	AckedOffset   uint64 `json:"acked_offset"`

	// Finish is the durable final-result state.
	Finish finishSessionState `json:"finish"`
}

// deliverySession is the persistent delivery session for one job.
//
// Chunk data lives in an append-only write-ahead log so that chunk records can
// never be clobbered by two processes taking the session over concurrently.
// Only the holder of the session lock may append new records, which guarantees
// each sequence number is ever associated with one chunk body. The JSON state
// file records progress (epoch, start/finish state and the acknowledged
// prefix); every state write is atomic and fenced by the epoch.
type deliverySession struct {
	client  *Client
	job     *api.Job
	logger  logger.Logger
	dir     string
	stateAt string
	walAt   string
	lock    *flock.Flock

	staleLockTimeout time.Duration

	// mu serializes all delivery operations within this process, which gives
	// multiple writers one deterministic delivery order.
	mu sync.Mutex

	state *jobSessionState
	wal   *os.File

	// dead is set once this session has been fenced out by a newer session.
	dead bool
}

// newDeliverySession opens (or creates and takes over) the persistent delivery
// session for a job.
func newDeliverySession(client *Client, job *api.Job, opts jobControllerOptions) (*deliverySession, error) {
	if job == nil || job.ID == "" {
		return nil, errors.New("cannot create a job delivery session without a job ID")
	}

	dir := opts.stateDir
	if dir == "" {
		d, err := defaultJobStateDir()
		if err != nil {
			return nil, err
		}
		dir = d
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("creating job session state directory %q: %w", dir, err)
	}

	var log logger.Logger = logger.Discard
	if client != nil && client.Logger != nil {
		log = client.Logger
	}

	name := fmt.Sprintf("%x", job.ID)
	s := &deliverySession{
		client:           client,
		job:              job,
		logger:           log,
		dir:              dir,
		stateAt:          filepath.Join(dir, name+".json"),
		walAt:            filepath.Join(dir, name+".wal"),
		lock:             flock.New(filepath.Join(dir, name+".lock")),
		staleLockTimeout: opts.staleLockTimeout,
	}
	if s.staleLockTimeout <= 0 {
		s.staleLockTimeout = defaultStaleLockTimeout
	}

	locked, err := s.lock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("acquiring job session lock for %s: %w", job.ID, err)
	}

	state, existed, err := s.loadState()
	if err != nil {
		return nil, err
	}

	if !locked && !existed {
		// A live process holds the lock but has not durably created a session
		// yet. Wait for it to finish initializing, then reload.
		ctx, cancel := context.WithTimeout(context.Background(), s.staleLockTimeout)
		got, lerr := s.lock.TryLockContext(ctx, 100*time.Millisecond)
		cancel()
		switch {
		case lerr != nil:
			return nil, fmt.Errorf("acquiring job session lock for %s: %w", job.ID, lerr)
		case got:
			locked = true
		}
		state, existed, err = s.loadState()
		if err != nil {
			return nil, err
		}
		if !locked && !existed {
			return nil, fmt.Errorf("another process holds the session lock for job %s but no session state exists; refusing to start a competing session", job.ID)
		}
	}

	if !existed {
		state = &jobSessionState{
			SchemaVersion: sessionSchemaVersion,
			JobID:         job.ID,
			Epoch:         1,
		}
	} else {
		// Taking over an existing session: bump the epoch so the previous
		// owner's next commit is rejected.
		state.Epoch++
	}

	// Open the write-ahead log before publishing the new epoch, and
	// reconcile the sequence/offset counters from its records.
	wal, err := os.OpenFile(s.walAt, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening job chunk log for %s: %w", job.ID, err)
	}
	s.wal = wal

	if err := s.reconcileLocked(state); err != nil {
		if cerr := wal.Close(); cerr != nil {
			return nil, fmt.Errorf("%w (and closing the chunk log: %v)", err, cerr)
		}
		return nil, err
	}
	s.state = state

	if !locked {
		s.logger.Warnf("The session for job %s is locked by another live process; fencing its epoch and taking over recovery (new epoch %d)", job.ID, state.Epoch)
	}

	// Publish the new epoch. This write is unconditional on purpose: the
	// epoch bump is the fence. Pending chunk data lives in the write-ahead
	// log, so racing with the previous owner can only duplicate delivery of
	// identical chunks, never lose or reorder one.
	if err := s.atomicWrite(state); err != nil {
		if cerr := wal.Close(); cerr != nil {
			return nil, fmt.Errorf("publishing job session for %s: %w (and closing the chunk log: %v)", job.ID, err, cerr)
		}
		return nil, fmt.Errorf("publishing job session for %s: %w", job.ID, err)
	}

	return s, nil
}

// Close releases the session lock and closes the chunk log. It does not remove
// the on-disk state: a later process may still need it to converge.
func (s *deliverySession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var err error
	if s.wal != nil {
		err = s.wal.Close()
		s.wal = nil
	}
	if s.lock.Locked() {
		if uerr := s.lock.Unlock(); uerr != nil && err == nil {
			err = uerr
		}
	}
	return err
}

// aliveLocked returns an error if the session may no longer be driven.
func (s *deliverySession) aliveLocked() error {
	if s.dead {
		return ErrSessionSuperseded
	}
	return nil
}

// commitLocked durably writes the session state. The on-disk epoch must still
// match this session's epoch; otherwise the session has been superseded and
// the controller is marked dead. Callers must hold s.mu.
func (s *deliverySession) commitLocked() error {
	if s.dead {
		return ErrSessionSuperseded
	}

	// Opportunistically take the lock back once a fenced owner has exited.
	if !s.lock.Locked() {
		if got, err := s.lock.TryLock(); err == nil && got {
			s.logger.Debugf("Acquired the session lock for job %s", s.job.ID)
		}
	}

	disk, existed, err := s.loadState()
	if err != nil {
		return err
	}
	if !existed || disk.Epoch != s.state.Epoch {
		s.dead = true
		return ErrSessionSuperseded
	}
	return s.atomicWrite(s.state)
}

// ensureLockForEnqueueLocked makes sure this session owns the lock before a new
// sequence is assigned. It tries to acquire the lock if a fenced previous
// owner has since exited.
func (s *deliverySession) ensureLockForEnqueueLocked() error {
	if s.lock.Locked() {
		return nil
	}
	got, err := s.lock.TryLock()
	if err != nil {
		return fmt.Errorf("acquiring job session lock for %s: %w", s.job.ID, err)
	}
	if !got {
		return ErrSessionLocked
	}
	// The previous owner is gone. Its last records may have reached the log
	// after our takeover snapshot; reconcile before assigning a sequence.
	if err := s.reconcileLocked(s.state); err != nil {
		return err
	}
	return nil
}

// enqueueChunkLocked durably appends a new log chunk and assigns it the next
// sequence and offset. Only the lock holder may enqueue. Callers must hold
// s.mu.
func (s *deliverySession) enqueueChunkLocked(data []byte) (pendingChunkState, error) {
	if err := s.aliveLocked(); err != nil {
		return pendingChunkState{}, err
	}
	if s.state.Finish.Acked {
		return pendingChunkState{}, ErrJobFinished
	}
	if err := s.ensureLockForEnqueueLocked(); err != nil {
		return pendingChunkState{}, err
	}

	chunk := pendingChunkState{
		Sequence: s.state.NextSequence,
		Offset:   s.state.NextOffset,
		Size:     uint64(len(data)),
		Data:     data,
	}

	// Fence one last time, then append the record. Only this process appends
	// while it holds the lock.
	disk, existed, err := s.loadState()
	if err != nil {
		return pendingChunkState{}, err
	}
	if !existed || disk.Epoch != s.state.Epoch {
		s.dead = true
		return pendingChunkState{}, ErrSessionSuperseded
	}

	if err := appendWALRecord(s.wal, chunk); err != nil {
		return pendingChunkState{}, fmt.Errorf("appending log chunk for job %s: %w", s.job.ID, err)
	}

	s.state.NextSequence++
	s.state.NextOffset += chunk.Size

	if err := s.commitLocked(); err != nil {
		// The record is intentionally left in the log: a newer session may
		// already be reading it, and a contiguous extra record is always safe
		// to deliver at-least-once (the new owner rebuilds its counters from
		// the log when it takes the lock). Truncating here could instead
		// remove a record the new owner has already acknowledged.
		return pendingChunkState{}, err
	}

	return chunk, nil
}

// pendingChunksLocked returns the chunks not yet acknowledged, in deterministic
// delivery (sequence) order. The log is re-read each time, so records appended
// by a fenced-out previous process are recovered too. Callers must hold s.mu.
func (s *deliverySession) pendingChunksLocked() ([]pendingChunkState, error) {
	if err := s.aliveLocked(); err != nil {
		return nil, err
	}
	chunks, err := readWAL(s.walAt)
	if err != nil {
		return nil, err
	}
	if s.state.AckedSequence > uint64(len(chunks)) {
		return nil, fmt.Errorf("job %s session state is inconsistent: %d chunks acknowledged but only %d records exist", s.job.ID, s.state.AckedSequence, len(chunks))
	}
	return chunks[s.state.AckedSequence:], nil
}

// ackChunkLocked records a successful chunk acknowledgement and persists the
// new acknowledged prefix. Callers must hold s.mu.
func (s *deliverySession) ackChunkLocked(chunk pendingChunkState) error {
	if chunk.Sequence != s.state.AckedSequence {
		return fmt.Errorf("job %s acknowledging chunk %d out of order (next expected %d)", s.job.ID, chunk.Sequence, s.state.AckedSequence)
	}
	s.state.AckedSequence++
	s.state.AckedOffset += chunk.Size
	return s.commitLocked()
}

// finishAcknowledgedLocked truncates the now-fully-acknowledged chunk log.
// Callers must hold s.mu.
func (s *deliverySession) finishAcknowledgedLocked() error {
	if err := os.Truncate(s.walAt, 0); err != nil {
		s.logger.Warnf("Could not truncate the delivered chunk log for job %s: %v", s.job.ID, err)
	}
	return nil
}

// reconcileLocked validates the write-ahead log and rebuilds the next
// sequence/offset counters from its records. Callers must hold s.mu (or call
// before the session is published).
func (s *deliverySession) reconcileLocked(state *jobSessionState) error {
	chunks, err := readWAL(s.walAt)
	if err != nil {
		return err
	}

	var expectOffset uint64
	for i, ch := range chunks {
		if ch.Sequence != uint64(i) {
			return fmt.Errorf("job %s chunk log is not contiguous (expected sequence %d, got %d)", s.job.ID, i, ch.Sequence)
		}
		if ch.Offset != expectOffset {
			return fmt.Errorf("job %s chunk log offset mismatch at sequence %d (expected %d, got %d)", s.job.ID, i, expectOffset, ch.Offset)
		}
		if ch.Size != uint64(len(ch.Data)) {
			return fmt.Errorf("job %s chunk log size mismatch at sequence %d (header says %d, record is %d bytes)", s.job.ID, i, ch.Size, len(ch.Data))
		}
		expectOffset += ch.Size
	}

	state.NextSequence = uint64(len(chunks))
	state.NextOffset = expectOffset

	if state.AckedSequence > state.NextSequence {
		return fmt.Errorf("job %s session state is inconsistent: %d chunks acknowledged but only %d exist", s.job.ID, state.AckedSequence, state.NextSequence)
	}
	return nil
}

// loadState reads the JSON session state. A missing file is reported as
// existed == false.
func (s *deliverySession) loadState() (*jobSessionState, bool, error) {
	data, err := os.ReadFile(s.stateAt)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("reading job session state %q: %w", s.stateAt, err)
	}

	var state jobSessionState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, false, fmt.Errorf("parsing job session state %q: %w", s.stateAt, err)
	}
	if state.SchemaVersion != sessionSchemaVersion {
		return nil, false, fmt.Errorf("job session state %q has unsupported schema version %d (agent supports %d)", s.stateAt, state.SchemaVersion, sessionSchemaVersion)
	}
	if state.JobID != s.job.ID {
		return nil, false, fmt.Errorf("job session state %q belongs to a different job", s.stateAt)
	}
	return &state, true, nil
}

// atomicWrite writes state to a temporary file in the same directory, fsyncs
// it, and renames it over the state file. Rename is atomic, so readers (and a
// competing process) either see the previous complete state or the new one.
func (s *deliverySession) atomicWrite(state *jobSessionState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding job session state: %w", err)
	}

	tmp := fmt.Sprintf("%s.tmp-%d-%d", s.stateAt, os.Getpid(), walTempCounter.Add(1))
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("creating temporary session state %q: %w", tmp, err)
	}

	writeErr := writeAll(f, data)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	if cerr := f.Close(); writeErr == nil && cerr != nil {
		writeErr = cerr
	}
	if writeErr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("writing job session state %q: %w", s.stateAt, writeErr)
	}

	if err := os.Rename(tmp, s.stateAt); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replacing job session state %q: %w", s.stateAt, err)
	}
	syncParentDir(s.dir)
	return nil
}

// writeAll writes all of data to f, retrying on short writes.
func writeAll(f *os.File, data []byte) error {
	for len(data) > 0 {
		n, err := f.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// syncParentDir best-effort fsyncs the directory so a renamed state file
// survives a crash or power loss.
func syncParentDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}

// appendWALRecord appends one framed record: sequence, offset, size then data.
// The file is opened O_APPEND and only the lock holder appends, so records are
// never interleaved.
func appendWALRecord(f *os.File, chunk pendingChunkState) error {
	var header [walHeaderSize]byte
	binary.BigEndian.PutUint64(header[0:8], chunk.Sequence)
	binary.BigEndian.PutUint64(header[8:16], chunk.Offset)
	binary.BigEndian.PutUint64(header[16:24], chunk.Size)
	if err := writeAll(f, header[:]); err != nil {
		return err
	}
	return writeAll(f, chunk.Data)
}

// readWAL reads and validates the framing of the chunk write-ahead log.
func readWAL(path string) ([]pendingChunkState, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading job chunk log %q: %w", path, err)
	}

	var chunks []pendingChunkState
	for len(data) > 0 {
		if len(data) < walHeaderSize {
			return nil, fmt.Errorf("job chunk log %q is truncated in a record header", path)
		}
		chunk := pendingChunkState{
			Sequence: binary.BigEndian.Uint64(data[0:8]),
			Offset:   binary.BigEndian.Uint64(data[8:16]),
			Size:     binary.BigEndian.Uint64(data[16:24]),
		}
		data = data[walHeaderSize:]

		if chunk.Size > uint64(len(data)) {
			return nil, fmt.Errorf("job chunk log %q is truncated in record data (sequence %d)", path, chunk.Sequence)
		}
		chunk.Data = make([]byte, chunk.Size)
		copy(chunk.Data, data[:chunk.Size])
		data = data[chunk.Size:]
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func defaultJobStateDir() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil || base == "" {
		base = os.TempDir()
	}
	return filepath.Join(base, "buildkite-agent", "job-sessions"), nil
}

// startJobAcknowledged sends the job start, retrying as [Client.StartJob] does.
// If priorAttempt is true (the start was durably recorded before and the
// response may have been lost), a 422 Unprocessable Entity is treated as the
// server already having accepted the start, rather than an error: the outcome
// has converged.
func (c *Client) startJobAcknowledged(ctx context.Context, job *api.Job, priorAttempt bool) error {
	return roko.NewRetrier(
		roko.WithMaxAttempts(7),
		roko.WithStrategy(roko.Exponential(2*time.Second, 0)),
		roko.WithSleepFunc(c.RetrySleepFunc),
	).DoWithContext(ctx, func(rtr *roko.Retrier) error {
		response, err := c.APIClient.StartJob(ctx, job)
		if err != nil {
			if response != nil && response.StatusCode == http.StatusUnprocessableEntity && priorAttempt {
				rtr.Break()
				return nil
			}
			if response != nil && api.IsRetryableStatus(response) {
				c.warnf("%s (%s)", err, rtr)
				return err
			}
			if api.IsRetryableError(err) {
				c.warnf("%s (%s)", err, rtr)
				return err
			}
			c.warnf("Buildkite rejected the call to start the job (%s)", err)
			rtr.Break()
		}
		return err
	})
}

// finishJobAcknowledged sends the final result, retrying as [Client.FinishJob]
// does. If priorAttempt is true (the finish was durably recorded before and
// the response may have been lost), a 422 Unprocessable Entity means the job
// is already in a finished state server-side, so the outcome has converged.
func (c *Client) finishJobAcknowledged(ctx context.Context, job *api.Job, ignoreAgentInDispatches *bool, priorAttempt bool) error {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Hour)
	defer cancel()

	return roko.NewRetrier(
		roko.WithStrategy(roko.ExponentialSubsecond(2*time.Second)),
		roko.WithMaxAttempts(12), // 12 attempts ≈ 10 minutes of total sleep
		roko.WithJitter(),
		roko.WithSleepFunc(c.RetrySleepFunc),
	).DoWithContext(ctx, func(retrier *roko.Retrier) error {
		response, err := c.APIClient.FinishJob(ctx, job, ignoreAgentInDispatches)
		if err != nil {
			if response != nil && response.StatusCode == http.StatusUnprocessableEntity && priorAttempt {
				retrier.Break()
				return nil
			}
			// Non-retryable responses (e.g. 422 on a first attempt, or 401
			// when job tokens are being used) mean the job has been cancelled
			// or otherwise already finished.
			if !api.BreakOnNonRetryable(retrier, response, err) {
				c.warnf("%s (%s)", err, retrier)
			}
		}
		return err
	})
}

func (c *Client) warnf(format string, v ...any) {
	if c.Logger != nil {
		c.Logger.Warnf(format, v...)
	}
}
