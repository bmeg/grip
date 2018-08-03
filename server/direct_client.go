package server

import (
	"io"

	"github.com/bmeg/arachne/aql"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// QueryDirectClient is a shim to connect aql.Query client directly server
type QueryDirectClient struct {
	server aql.QueryServer
}

// NewQueryDirectClient creates new QueryDirectClient
func NewQueryDirectClient(server aql.QueryServer) *QueryDirectClient {
	return &QueryDirectClient{server}
}

//Aggregate shim
func (dir *QueryDirectClient) Aggregate(ctx context.Context, in *aql.AggregationsRequest, opts ...grpc.CallOption) (*aql.NamedAggregationResult, error) {
	return dir.server.Aggregate(ctx, in)
}

//GetVertex shim
func (dir *QueryDirectClient) GetVertex(ctx context.Context, in *aql.ElementID, opts ...grpc.CallOption) (*aql.Vertex, error) {
	return dir.server.GetVertex(ctx, in)
}

//GetEdge shim
func (dir *QueryDirectClient) GetEdge(ctx context.Context, in *aql.ElementID, opts ...grpc.CallOption) (*aql.Edge, error) {
	return dir.server.GetEdge(ctx, in)
}

//GetTimestamp shim
func (dir *QueryDirectClient) GetTimestamp(ctx context.Context, in *aql.GraphID, opts ...grpc.CallOption) (*aql.Timestamp, error) {
	return dir.server.GetTimestamp(ctx, in)
}

//GetSchema shim
func (dir *QueryDirectClient) GetSchema(ctx context.Context, in *aql.GraphID, opts ...grpc.CallOption) (*aql.GraphSchema, error) {
	return dir.server.GetSchema(ctx, in)
}

///// Query Traversal Send/Recv shim
type directQueryTraversal struct {
	ctx context.Context
	c   chan *aql.QueryResult
}

func (dqt *directQueryTraversal) Recv() (*aql.QueryResult, error) {
	value, ok := <-dqt.c
	if !ok {
		return nil, io.EOF
	}
	return value, nil
}

func (dqt *directQueryTraversal) Send(a *aql.QueryResult) error {
	dqt.c <- a
	return nil
}

func (dqt *directQueryTraversal) close() {
	close(dqt.c)
}

func (dqt *directQueryTraversal) Context() context.Context {
	return dqt.ctx
}

func (dqt *directQueryTraversal) CloseSend() error             { return nil }
func (dqt *directQueryTraversal) SetTrailer(metadata.MD)       {}
func (dqt *directQueryTraversal) SetHeader(metadata.MD) error  { return nil }
func (dqt *directQueryTraversal) SendHeader(metadata.MD) error { return nil }
func (dqt *directQueryTraversal) SendMsg(m interface{}) error  { return nil }
func (dqt *directQueryTraversal) RecvMsg(m interface{}) error  { return nil }
func (dqt *directQueryTraversal) Header() (metadata.MD, error) { return nil, nil }
func (dqt *directQueryTraversal) Trailer() metadata.MD         { return nil }

//Traversal shim
func (dir *QueryDirectClient) Traversal(ctx context.Context, in *aql.GraphQuery, opts ...grpc.CallOption) (aql.Query_TraversalClient, error) {
	w := &directQueryTraversal{ctx, make(chan *aql.QueryResult, 100)}
	go func() {
		dir.server.Traversal(in, w)
		w.close()
	}()
	return w, nil
}

///// Query ListGraphs Send/Recv shim

type directQueryListGraphs struct {
	ctx context.Context
	c   chan *aql.GraphID
}

func (dqt *directQueryListGraphs) Recv() (*aql.GraphID, error) {
	value, ok := <-dqt.c
	if !ok {
		return nil, io.EOF
	}
	return value, nil
}

func (dqt *directQueryListGraphs) Send(a *aql.GraphID) error {
	dqt.c <- a
	return nil
}

func (dqt *directQueryListGraphs) close() {
	close(dqt.c)
}

func (dqt *directQueryListGraphs) Context() context.Context {
	return dqt.ctx
}

func (dqt *directQueryListGraphs) CloseSend() error             { return nil }
func (dqt *directQueryListGraphs) SetTrailer(metadata.MD)       {}
func (dqt *directQueryListGraphs) SetHeader(metadata.MD) error  { return nil }
func (dqt *directQueryListGraphs) SendHeader(metadata.MD) error { return nil }
func (dqt *directQueryListGraphs) SendMsg(m interface{}) error  { return nil }
func (dqt *directQueryListGraphs) RecvMsg(m interface{}) error  { return nil }
func (dqt *directQueryListGraphs) Header() (metadata.MD, error) { return nil, nil }
func (dqt *directQueryListGraphs) Trailer() metadata.MD         { return nil }

// ListGraphs shim
func (dir *QueryDirectClient) ListGraphs(ctx context.Context, in *aql.Empty, opts ...grpc.CallOption) (aql.Query_ListGraphsClient, error) {
	w := &directQueryListGraphs{ctx, make(chan *aql.GraphID, 100)}
	go func() {
		dir.server.ListGraphs(in, w)
		w.close()
	}()
	return w, nil
}

///// Query ListIndices Send/Recv shim

type directQueryListIndices struct {
	ctx context.Context
	c   chan *aql.IndexID
}

func (dqt *directQueryListIndices) Recv() (*aql.IndexID, error) {
	value, ok := <-dqt.c
	if !ok {
		return nil, io.EOF
	}
	return value, nil
}

func (dqt *directQueryListIndices) Send(a *aql.IndexID) error {
	dqt.c <- a
	return nil
}

func (dqt *directQueryListIndices) close() {
	close(dqt.c)
}

func (dqt *directQueryListIndices) Context() context.Context {
	return dqt.ctx
}

func (dqt *directQueryListIndices) CloseSend() error             { return nil }
func (dqt *directQueryListIndices) SetTrailer(metadata.MD)       {}
func (dqt *directQueryListIndices) SetHeader(metadata.MD) error  { return nil }
func (dqt *directQueryListIndices) SendHeader(metadata.MD) error { return nil }
func (dqt *directQueryListIndices) SendMsg(m interface{}) error  { return nil }
func (dqt *directQueryListIndices) RecvMsg(m interface{}) error  { return nil }
func (dqt *directQueryListIndices) Header() (metadata.MD, error) { return nil, nil }
func (dqt *directQueryListIndices) Trailer() metadata.MD         { return nil }

// ListIndices shim
func (dir *QueryDirectClient) ListIndices(ctx context.Context, in *aql.GraphID, opts ...grpc.CallOption) (aql.Query_ListIndicesClient, error) {
	w := &directQueryListIndices{ctx, make(chan *aql.IndexID, 100)}
	go func() {
		dir.server.ListIndices(in, w)
		w.close()
	}()
	return w, nil
}
