package core

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/buildkite/agent/v4/api"
	agentedgev1 "github.com/buildkite/agent/v4/api/proto/gen"
	"github.com/buildkite/agent/v4/api/proto/gen/agentedgev1connect"
	"github.com/google/uuid"
)

// fakeControlPlane is a minimal in-process fake of the Buildkite Agent API
// for testing the core Controller: register/connect/disconnect, heartbeat,
// polling ping, job accept/start/finish and (optionally) streaming pings.
const (
	fakeRegToken     = "registration-token"
	fakeSessionToken = "session-token"
)

type fakeJob struct {
	id    string
	state string
	env   map[string]string
}

type fakeControlPlane struct {
	*httptest.Server

	mu sync.Mutex

	registeredIgnore *bool
	agentUUID        string
	connected        bool
	disconnected     bool

	pings      atomic.Int32
	heartbeats atomic.Int32

	paused     bool
	disconnect bool
	jobs       map[string]*fakeJob
	assignedID string // job ID returned by polling pings (until accepted)

	streaming       bool
	streamCh        chan *agentedgev1.StreamPingsResponse
	streamCloseOnce sync.Once
	streamHandler   func(context.Context, *connect.Request[agentedgev1.StreamPingsRequest], *connect.ServerStream[agentedgev1.StreamPingsResponse]) error
	streamConns     atomic.Int32
	streamAgentIDs  []string
}

func newFakeControlPlane(t *testing.T, streaming bool) *fakeControlPlane {
	t.Helper()

	f := &fakeControlPlane{
		agentUUID: uuid.New().String(),
		jobs:      make(map[string]*fakeJob),
		streaming: streaming,
	}
	if streaming {
		f.streamCh = make(chan *agentedgev1.StreamPingsResponse)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v3/register", f.handleRegister)
	mux.HandleFunc("POST /v3/connect", f.handleConnect)
	mux.HandleFunc("POST /v3/disconnect", f.handleDisconnect)
	mux.HandleFunc("POST /v3/heartbeat", f.handleHeartbeat)
	mux.HandleFunc("GET /v3/ping", f.handlePing)
	mux.HandleFunc("PUT /v3/jobs/{id}/acquire", f.handleAcquire)
	mux.HandleFunc("PUT /v3/jobs/{id}/accept", f.handleAccept)
	mux.HandleFunc("PUT /v3/jobs/{id}/start", f.handleStart)
	mux.HandleFunc("PUT /v3/jobs/{id}/finish", f.handleFinish)

	// The streaming service is always mounted; when streaming is disabled it
	// responds with CodeUnimplemented, emulating a server without the RPC.
	_, handler := agentedgev1connect.NewAgentEdgeServiceHandler(f)
	mux.Handle("/agentedge.v1.AgentEdgeService/", handler)

	f.Server = httptest.NewServer(mux)
	t.Cleanup(f.Close)
	t.Cleanup(f.closeStream)
	return f
}

// closeStream ends any StreamPings handler still ranging over streamCh.
func (f *fakeControlPlane) closeStream() {
	f.streamCloseOnce.Do(func() {
		if f.streamCh != nil {
			close(f.streamCh)
		}
	})
}

func (f *fakeControlPlane) writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		panic(err)
	}
}

func (f *fakeControlPlane) fakeMsg(msg string) string {
	b, err := json.Marshal(map[string]string{"message": msg})
	if err != nil {
		panic(err)
	}
	return string(b)
}

func (f *fakeControlPlane) handleRegister(w http.ResponseWriter, r *http.Request) {
	if got := r.Header.Get("Authorization"); got != "Token "+fakeRegToken {
		http.Error(w, f.fakeMsg("bad registration token"), http.StatusUnauthorized)
		return
	}
	var req api.AgentRegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, f.fakeMsg(err.Error()), http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	ignore := req.IgnoreInDispatches
	f.registeredIgnore = &ignore
	f.mu.Unlock()

	f.writeJSON(w, &api.AgentRegisterResponse{
		UUID:              f.agentUUID,
		Name:              req.Name,
		Tags:              req.Tags,
		AccessToken:       fakeSessionToken,
		Endpoint:          f.URL + "/v3",
		PingInterval:      1,
		JobStatusInterval: 5,
		HeartbeatInterval: 3600,
	})
}

func (f *fakeControlPlane) handleConnect(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	f.connected = true
	f.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (f *fakeControlPlane) handleDisconnect(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	f.disconnected = true
	f.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (f *fakeControlPlane) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	f.heartbeats.Add(1)
	f.writeJSON(w, &api.Heartbeat{
		SentAt:     time.Now().Format(time.RFC3339Nano),
		ReceivedAt: time.Now().Format(time.RFC3339Nano),
	})
}

func (f *fakeControlPlane) handlePing(w http.ResponseWriter, r *http.Request) {
	f.pings.Add(1)

	f.mu.Lock()
	defer f.mu.Unlock()

	var ping api.Ping
	switch {
	case f.disconnect:
		ping.Action = "disconnect"
	case f.paused:
		ping.Action = "pause"
	case f.assignedID != "":
		if job := f.jobs[f.assignedID]; job != nil && job.state == "assigned" {
			ping.Job = f.apiJob(job)
		}
	}
	f.writeJSON(w, &ping)
}

func (f *fakeControlPlane) apiJob(j *fakeJob) *api.Job {
	return &api.Job{
		ID:                 j.id,
		State:              j.state,
		Env:                j.env,
		ChunksMaxSizeBytes: 1024,
	}
}

func (f *fakeControlPlane) handleAcquire(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	job := f.jobs[r.PathValue("id")]
	if job == nil {
		http.Error(w, f.fakeMsg("no such job"), http.StatusNotFound)
		return
	}
	if job.state != "scheduled" {
		http.Error(w, f.fakeMsg("job not schedulable"), http.StatusUnprocessableEntity)
		return
	}
	job.state = "accepted"
	f.writeJSON(w, f.apiJob(job))
}

func (f *fakeControlPlane) handleAccept(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	job := f.jobs[r.PathValue("id")]
	if job == nil {
		http.Error(w, f.fakeMsg("no such job"), http.StatusNotFound)
		return
	}
	// Streaming dispatches may arrive without a polling assignment.
	if job.state != "assigned" && job.state != "scheduled" {
		http.Error(w, f.fakeMsg("job not acceptable"), http.StatusUnprocessableEntity)
		return
	}
	job.state = "accepted"
	f.assignedID = ""
	f.writeJSON(w, f.apiJob(job))
}

func (f *fakeControlPlane) handleStart(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	job := f.jobs[r.PathValue("id")]
	if job == nil || job.state != "accepted" {
		http.Error(w, f.fakeMsg("job not started"), http.StatusUnprocessableEntity)
		return
	}
	job.state = "running"
	w.WriteHeader(http.StatusOK)
}

func (f *fakeControlPlane) handleFinish(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	job := f.jobs[r.PathValue("id")]
	if job == nil || job.state != "running" {
		http.Error(w, f.fakeMsg("job not running"), http.StatusUnprocessableEntity)
		return
	}
	job.state = "finished"
	w.WriteHeader(http.StatusOK)
}

func (f *fakeControlPlane) addJob() *fakeJob {
	j := &fakeJob{
		id:    uuid.New().String(),
		state: "scheduled",
		env:   map[string]string{"BUILDKITE_COMMAND": "echo hello"},
	}
	f.mu.Lock()
	f.jobs[j.id] = j
	f.mu.Unlock()
	return j
}

func (f *fakeControlPlane) assign(j *fakeJob) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j.state = "assigned"
	f.assignedID = j.id
}

func (f *fakeControlPlane) jobState(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if j := f.jobs[id]; j != nil {
		return j.state
	}
	return ""
}

func (f *fakeControlPlane) setPaused(paused bool) {
	f.mu.Lock()
	f.paused = paused
	f.mu.Unlock()
}

func (f *fakeControlPlane) setDisconnect() {
	f.mu.Lock()
	f.disconnect = true
	f.mu.Unlock()
	f.closeStream()
}

// StreamPings implements the AgentEdgeService ConnectRPC handler.
func (f *fakeControlPlane) StreamPings(
	ctx context.Context,
	req *connect.Request[agentedgev1.StreamPingsRequest],
	resp *connect.ServerStream[agentedgev1.StreamPingsResponse],
) error {
	if req.Header().Get("Authorization") != "Token "+fakeSessionToken {
		return connect.NewError(connect.CodePermissionDenied, errors.New("bad session token"))
	}

	n := f.streamConns.Add(1)
	f.mu.Lock()
	f.streamAgentIDs = append(f.streamAgentIDs, req.Msg.GetAgentId())
	f.mu.Unlock()

	if !f.streaming {
		return connect.NewError(connect.CodeUnimplemented, errors.New("streaming dispatch not supported"))
	}
	if f.streamHandler != nil {
		return f.streamHandler(ctx, req, resp)
	}
	_ = n
	for msg := range f.streamCh {
		if err := resp.Send(msg); err != nil {
			return connect.NewError(connect.CodeUnknown, err)
		}
	}
	return nil
}

// waitFor polls cond until it is true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}
