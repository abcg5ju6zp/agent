package core

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/buildkite/agent/v4/api"
)

// JobController provides simple functionality to a job runner - the ability to
// write job logs and exit with a status code.
//
// Every JobController owns a recoverable delivery session for its job, stored
// on local disk. Log chunk sequence numbers, byte offsets, the unacknowledged
// chunk data, and the job's start and finish state are persisted before they
// are sent. If a request's fate is unknown (a network failure, or the process
// dying before a response arrives), the same chunk, start, or finish is sent
// again with its original sequence and timestamps rather than generating a
// new one. Multiple concurrent writers are serialized into one deterministic
// delivery order.
//
// After a process restart, a newly created JobController for the same job
// resumes the session. [JobController.Resume] delivers any chunks that were
// not acknowledged (and re-sends an unacknowledged start or finish), and
// [JobController.Finish] only submits the final result once every log chunk
// has been acknowledged.
type JobController struct {
	client  *Client
	job     *api.Job
	session *deliverySession
}

// NewJobController creates a new job controller for a job, using the default
// on-disk session state directory. Session files are shared across restarts
// of the controlling process (keyed by job ID).
func NewJobController(client *Client, job *api.Job) (*JobController, error) {
	return NewJobControllerWithOptions(client, job)
}

// NewJobControllerWithOptions creates a new job controller for a job with
// options, such as the on-disk session state directory.
func NewJobControllerWithOptions(client *Client, job *api.Job, opts ...JobControllerOption) (*JobController, error) {
	options := jobControllerOptions{
		stateDir:         "",
		staleLockTimeout: defaultStaleLockTimeout,
	}
	for _, o := range opts {
		o(&options)
	}

	session, err := newDeliverySession(client, job, options)
	if err != nil {
		return nil, err
	}

	return &JobController{
		client:  client,
		job:     job,
		session: session,
	}, nil
}

// WriteLog writes some log content for this job back to Buildkite.
//
// Unlike the main agent, this immediately uploads the content without any
// buffering, does not tee the log to a local file, nor does it insert
// timestamp codes. It uses the same retry loop as the agent, which can attempt
// to upload logs for a very long time.
//
// The chunk is persisted locally (with its sequence and offset) before it is
// uploaded, and it is only considered delivered once Buildkite acknowledges
// it. If the response is lost - including because the process exited mid
// request - the next WriteLog, Finish, or Resume retries the same chunk rather
// than assigning a new sequence. Concurrent WriteLog calls are delivered in
// the deterministic order in which they acquire the controller.
func (c *JobController) WriteLog(ctx context.Context, log string) error {
	s := c.session
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.enqueueChunkLocked([]byte(log)); err != nil {
		return err
	}
	return c.drainPendingLocked(ctx)
}

// Start marks a job as started in Buildkite. If an error is returned, the job
// should not proceed.
//
// The start (including the StartedAt timestamp) is persisted locally before it
// is sent. If a previous attempt's response was lost, including across a
// process restart, Start re-sends the same start with the original timestamp
// instead of recording a new one. Once acknowledged, repeated Start calls
// converge to success without contacting Buildkite again.
func (c *JobController) Start(ctx context.Context) error {
	c.session.mu.Lock()
	defer c.session.mu.Unlock()

	state := c.session.state
	if state.StartAcked {
		return nil
	}

	priorAttempt := state.Started
	if !priorAttempt {
		state.Started = true
		state.StartedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if err := c.session.commitLocked(); err != nil {
			return err
		}
	}

	return c.sendStartLocked(ctx, priorAttempt)
}

// Resume drives a session opened by a previous (possibly crashed) process
// towards convergence: it re-sends an unacknowledged start, delivers all
// persisted but unacknowledged log chunks in order (retrying each chunk with
// its original sequence and data), and re-sends a finish that was requested
// but not acknowledged. It returns nil when there is nothing left to do.
//
// A finish is only sent after every log chunk has been acknowledged. If any
// chunk cannot be delivered, Resume returns its error without finishing.
func (c *JobController) Resume(ctx context.Context) error {
	c.session.mu.Lock()
	defer c.session.mu.Unlock()

	state := c.session.state
	if state.Finish.Acked {
		return nil // already converged to a terminal job
	}
	if state.Started && !state.StartAcked {
		if err := c.sendStartLocked(ctx, true); err != nil {
			return err
		}
	}

	if err := c.drainPendingLocked(ctx); err != nil {
		return err
	}

	if state.Finish.Requested && !state.Finish.Acked {
		return c.sendFinishLocked(ctx, true)
	}
	return nil
}

// Finish marks the job as finished in Buildkite.
//
// The final result (exit status, signal, signal reason and FinishedAt
// timestamp) is persisted locally first, so a forced exit or a lost response
// still converges after restart. Finish then waits until every log chunk has
// been acknowledged before submitting the final result exactly once. If the
// finish response is lost, a later Finish or Resume re-sends the same final
// result with its original timestamp. Once acknowledged, repeated Finish
// calls converge to success without contacting Buildkite again.
//
// A cancelled or force-killed job is expressed through ProcessExit (for
// example Signal "KILL", or SignalReason [SignalReasonCancel]) and is made
// recoverable in exactly the same way.
func (c *JobController) Finish(ctx context.Context, exit ProcessExit, ignoreAgentInDispatches *bool) error {
	c.session.mu.Lock()
	defer c.session.mu.Unlock()

	state := c.session.state
	if state.Finish.Acked {
		return nil
	}

	priorAttempt := state.Finish.Requested
	if !priorAttempt {
		state.Finish.Requested = true
		state.Finish.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		state.Finish.ExitStatus = exit.Status
		state.Finish.Signal = exit.Signal
		state.Finish.SignalReason = exit.SignalReason
		state.Finish.IgnoreAgentInDispatches = ignoreAgentInDispatches
		if err := c.session.commitLocked(); err != nil {
			return err
		}
	}

	// All logs must be remotely acknowledged before submitting the final
	// result.
	if err := c.drainPendingLocked(ctx); err != nil {
		return err
	}

	return c.sendFinishLocked(ctx, priorAttempt)
}

// drainPendingLocked uploads every unacknowledged chunk in sequence order,
// advancing the persisted acknowledged prefix only after acknowledgement.
func (c *JobController) drainPendingLocked(ctx context.Context) error {
	for {
		pending, err := c.session.pendingChunksLocked()
		if err != nil {
			return err
		}
		if len(pending) == 0 {
			return nil
		}

		head := pending[0]
		err = c.client.UploadChunk(ctx, c.job.ID, &api.Chunk{
			Data:     head.Data,
			Sequence: head.Sequence,
			Offset:   head.Offset,
			Size:     head.Size,
		})
		if err != nil {
			// The chunk stays persisted and pending; the next WriteLog,
			// Resume, or Finish retries the very same chunk.
			return err
		}

		if err := c.session.ackChunkLocked(head); err != nil {
			return err
		}
	}
}

// sendStartLocked performs the start API call using the persisted timestamp
// and records acknowledgement.
func (c *JobController) sendStartLocked(ctx context.Context, priorAttempt bool) error {
	state := c.session.state
	c.job.StartedAt = state.StartedAt
	if err := c.client.startJobAcknowledged(ctx, c.job, priorAttempt); err != nil {
		return err
	}
	state.StartAcked = true
	return c.session.commitLocked()
}

// sendFinishLocked performs the finish API call using the persisted final
// result and records acknowledgement.
func (c *JobController) sendFinishLocked(ctx context.Context, priorAttempt bool) error {
	state := c.session.state
	c.job.FinishedAt = state.Finish.FinishedAt
	c.job.ExitStatus = strconv.Itoa(state.Finish.ExitStatus)
	c.job.Signal = state.Finish.Signal
	c.job.SignalReason = state.Finish.SignalReason
	c.job.ChunksFailedCount = 0

	if err := c.client.finishJobAcknowledged(ctx, c.job, state.Finish.IgnoreAgentInDispatches, priorAttempt); err != nil {
		return err
	}

	state.Finish.Acked = true
	if err := c.session.commitLocked(); err != nil {
		return err
	}
	return c.session.finishAcknowledgedLocked()
}

// Close releases the on-disk session lock. It does not remove the session
// state: a later process may still need it to converge.
func (c *JobController) Close() error {
	return c.session.Close()
}

// BKTimestamp formats a time as a Buildkite timestamp code.
func BKTimestamp(t time.Time) string {
	return fmt.Sprintf("\x1b_bk;t=%d\x07", t.UnixNano()/int64(time.Millisecond))
}
