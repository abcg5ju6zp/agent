package core

import (
	"context"
	"iter"

	"connectrpc.com/connect"
	"github.com/buildkite/agent/v4/api"
	agentedgev1 "github.com/buildkite/agent/v4/api/proto/gen"
)

// APIClient defines the subset of client methods needed by core.
type APIClient interface {
	AcceptJob(context.Context, string) (*api.Job, *api.Response, error)
	AcquireJob(context.Context, string, ...api.Header) (*api.Job, *api.Response, error)
	Connect(context.Context) (*api.Response, error)
	Disconnect(context.Context) (*api.Response, error)
	FinishJob(context.Context, *api.Job, *bool) (*api.Response, error)
	Heartbeat(context.Context) (*api.Heartbeat, *api.Response, error)
	Ping(context.Context) (*api.Ping, *api.Response, error)
	Register(context.Context, *api.AgentRegisterRequest) (*api.AgentRegisterResponse, *api.Response, error)
	StartJob(context.Context, *api.Job) (*api.Response, error)
	StreamPings(context.Context, string, ...connect.ClientOption) (iter.Seq2[*agentedgev1.StreamPingsResponse, error], error)
	UploadChunk(context.Context, string, *api.Chunk) (*api.Response, error)
}
