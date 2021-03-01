
// Code generated by protoc-gen-grpc-rest-direct. DO NOT EDIT.
package gripql

import (
	"io"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)


// QueryDirectClient is a shim to connect Query client directly server
type QueryDirectClient struct {
	server QueryServer
}
 // NewQueryDirectClient creates new QueryDirectClient
func NewQueryDirectClient(server QueryServer) *QueryDirectClient {
	return &QueryDirectClient{server}
}

//Traversal streaming output shim
type directQueryTraversal struct {
  ctx context.Context
  c   chan *QueryResult
  e   error
}

func (dsm *directQueryTraversal) Recv() (*QueryResult, error) {
	value, ok := <-dsm.c
	if !ok {
    if dsm.e != nil {
      return nil, dsm.e
    }
		return nil, io.EOF
	}
	return value, dsm.e
}
func (dsm *directQueryTraversal) Send(a *QueryResult) error {
	dsm.c <- a
	return nil
}
func (dsm *directQueryTraversal) close() {
	close(dsm.c)
}
func (dsm *directQueryTraversal) Context() context.Context {
	return dsm.ctx
}
func (dsm *directQueryTraversal) CloseSend() error             { return nil }
func (dsm *directQueryTraversal) SetTrailer(metadata.MD)       {}
func (dsm *directQueryTraversal) SetHeader(metadata.MD) error  { return nil }
func (dsm *directQueryTraversal) SendHeader(metadata.MD) error { return nil }
func (dsm *directQueryTraversal) SendMsg(m interface{}) error  { return nil }
func (dsm *directQueryTraversal) RecvMsg(m interface{}) error  { return nil }
func (dsm *directQueryTraversal) Header() (metadata.MD, error) { return nil, nil }
func (dsm *directQueryTraversal) Trailer() metadata.MD         { return nil }
func (dir *QueryDirectClient) Traversal(ctx context.Context, in *GraphQuery, opts ...grpc.CallOption) (Query_TraversalClient, error) {
	w := &directQueryTraversal{ctx, make(chan *QueryResult, 100), nil}
	go func() {
    defer w.close()
		w.e = dir.server.Traversal(in, w)
	}()
	return w, nil
}


//GetResults streaming output shim
type directQueryGetResults struct {
  ctx context.Context
  c   chan *QueryResult
  e   error
}

func (dsm *directQueryGetResults) Recv() (*QueryResult, error) {
	value, ok := <-dsm.c
	if !ok {
    if dsm.e != nil {
      return nil, dsm.e
    }
		return nil, io.EOF
	}
	return value, dsm.e
}
func (dsm *directQueryGetResults) Send(a *QueryResult) error {
	dsm.c <- a
	return nil
}
func (dsm *directQueryGetResults) close() {
	close(dsm.c)
}
func (dsm *directQueryGetResults) Context() context.Context {
	return dsm.ctx
}
func (dsm *directQueryGetResults) CloseSend() error             { return nil }
func (dsm *directQueryGetResults) SetTrailer(metadata.MD)       {}
func (dsm *directQueryGetResults) SetHeader(metadata.MD) error  { return nil }
func (dsm *directQueryGetResults) SendHeader(metadata.MD) error { return nil }
func (dsm *directQueryGetResults) SendMsg(m interface{}) error  { return nil }
func (dsm *directQueryGetResults) RecvMsg(m interface{}) error  { return nil }
func (dsm *directQueryGetResults) Header() (metadata.MD, error) { return nil, nil }
func (dsm *directQueryGetResults) Trailer() metadata.MD         { return nil }
func (dir *QueryDirectClient) GetResults(ctx context.Context, in *QueryJob, opts ...grpc.CallOption) (Query_GetResultsClient, error) {
	w := &directQueryGetResults{ctx, make(chan *QueryResult, 100), nil}
	go func() {
    defer w.close()
		w.e = dir.server.GetResults(in, w)
	}()
	return w, nil
}


//GetVertex shim
func (shim *QueryDirectClient) GetVertex(ctx context.Context, in *ElementID, opts ...grpc.CallOption) (*Vertex, error) {
	return shim.server.GetVertex(ctx, in)
}

//GetEdge shim
func (shim *QueryDirectClient) GetEdge(ctx context.Context, in *ElementID, opts ...grpc.CallOption) (*Edge, error) {
	return shim.server.GetEdge(ctx, in)
}

//GetTimestamp shim
func (shim *QueryDirectClient) GetTimestamp(ctx context.Context, in *GraphID, opts ...grpc.CallOption) (*Timestamp, error) {
	return shim.server.GetTimestamp(ctx, in)
}

//GetSchema shim
func (shim *QueryDirectClient) GetSchema(ctx context.Context, in *GraphID, opts ...grpc.CallOption) (*Graph, error) {
	return shim.server.GetSchema(ctx, in)
}

//ListGraphs shim
func (shim *QueryDirectClient) ListGraphs(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*ListGraphsResponse, error) {
	return shim.server.ListGraphs(ctx, in)
}

//ListIndices shim
func (shim *QueryDirectClient) ListIndices(ctx context.Context, in *GraphID, opts ...grpc.CallOption) (*ListIndicesResponse, error) {
	return shim.server.ListIndices(ctx, in)
}

//ListLabels shim
func (shim *QueryDirectClient) ListLabels(ctx context.Context, in *GraphID, opts ...grpc.CallOption) (*ListLabelsResponse, error) {
	return shim.server.ListLabels(ctx, in)
}

// JobDirectClient is a shim to connect Job client directly server
type JobDirectClient struct {
	server JobServer
}
 // NewJobDirectClient creates new JobDirectClient
func NewJobDirectClient(server JobServer) *JobDirectClient {
	return &JobDirectClient{server}
}

//Job shim
func (shim *JobDirectClient) Job(ctx context.Context, in *GraphQuery, opts ...grpc.CallOption) (*QueryJob, error) {
	return shim.server.Job(ctx, in)
}

//ListJobs streaming output shim
type directJobListJobs struct {
  ctx context.Context
  c   chan *QueryJob
  e   error
}

func (dsm *directJobListJobs) Recv() (*QueryJob, error) {
	value, ok := <-dsm.c
	if !ok {
    if dsm.e != nil {
      return nil, dsm.e
    }
		return nil, io.EOF
	}
	return value, dsm.e
}
func (dsm *directJobListJobs) Send(a *QueryJob) error {
	dsm.c <- a
	return nil
}
func (dsm *directJobListJobs) close() {
	close(dsm.c)
}
func (dsm *directJobListJobs) Context() context.Context {
	return dsm.ctx
}
func (dsm *directJobListJobs) CloseSend() error             { return nil }
func (dsm *directJobListJobs) SetTrailer(metadata.MD)       {}
func (dsm *directJobListJobs) SetHeader(metadata.MD) error  { return nil }
func (dsm *directJobListJobs) SendHeader(metadata.MD) error { return nil }
func (dsm *directJobListJobs) SendMsg(m interface{}) error  { return nil }
func (dsm *directJobListJobs) RecvMsg(m interface{}) error  { return nil }
func (dsm *directJobListJobs) Header() (metadata.MD, error) { return nil, nil }
func (dsm *directJobListJobs) Trailer() metadata.MD         { return nil }
func (dir *JobDirectClient) ListJobs(ctx context.Context, in *Graph, opts ...grpc.CallOption) (Job_ListJobsClient, error) {
	w := &directJobListJobs{ctx, make(chan *QueryJob, 100), nil}
	go func() {
    defer w.close()
		w.e = dir.server.ListJobs(in, w)
	}()
	return w, nil
}


//DeleteJob shim
func (shim *JobDirectClient) DeleteJob(ctx context.Context, in *QueryJob, opts ...grpc.CallOption) (*JobStatus, error) {
	return shim.server.DeleteJob(ctx, in)
}

//GetJob shim
func (shim *JobDirectClient) GetJob(ctx context.Context, in *QueryJob, opts ...grpc.CallOption) (*JobStatus, error) {
	return shim.server.GetJob(ctx, in)
}

// EditDirectClient is a shim to connect Edit client directly server
type EditDirectClient struct {
	server EditServer
}
 // NewEditDirectClient creates new EditDirectClient
func NewEditDirectClient(server EditServer) *EditDirectClient {
	return &EditDirectClient{server}
}

//AddVertex shim
func (shim *EditDirectClient) AddVertex(ctx context.Context, in *GraphElement, opts ...grpc.CallOption) (*EditResult, error) {
	return shim.server.AddVertex(ctx, in)
}

//AddEdge shim
func (shim *EditDirectClient) AddEdge(ctx context.Context, in *GraphElement, opts ...grpc.CallOption) (*EditResult, error) {
	return shim.server.AddEdge(ctx, in)
}

//BulkAdd streaming input shim
type directEditBulkAdd struct {
  ctx context.Context
  c   chan *GraphElement
  out chan *BulkEditResult
}

func (dsm *directEditBulkAdd) Recv() (*GraphElement, error) {
	value, ok := <-dsm.c
	if !ok {
		return nil, io.EOF
	}
	return value, nil
}

func (dsm *directEditBulkAdd) Send(a *GraphElement) error {
	dsm.c <- a
	return nil
}

func (dsm *directEditBulkAdd) Context() context.Context {
	return dsm.ctx
}

func (dsm *directEditBulkAdd) SendAndClose(o *BulkEditResult) error {
  dsm.out <- o
  close(dsm.out)
  return nil
}

func (dsm *directEditBulkAdd) CloseAndRecv() (*BulkEditResult, error) {
  close(dsm.c)
  out := <- dsm.out
  return out, nil
}

func (dsm *directEditBulkAdd) CloseSend() error             { return nil }
func (dsm *directEditBulkAdd) SetTrailer(metadata.MD)       {}
func (dsm *directEditBulkAdd) SetHeader(metadata.MD) error  { return nil }
func (dsm *directEditBulkAdd) SendHeader(metadata.MD) error { return nil }
func (dsm *directEditBulkAdd) SendMsg(m interface{}) error  { return nil }
func (dsm *directEditBulkAdd) RecvMsg(m interface{}) error  { return nil }
func (dsm *directEditBulkAdd) Header() (metadata.MD, error) { return nil, nil }
func (dsm *directEditBulkAdd) Trailer() metadata.MD         { return nil }

func (dir *EditDirectClient) BulkAdd(ctx context.Context, opts ...grpc.CallOption) (Edit_BulkAddClient, error) {
	w := &directEditBulkAdd{ctx, make(chan *GraphElement, 100), make(chan *BulkEditResult, 3)}
	go func() {
		dir.server.BulkAdd(w)
	}()
	return w, nil
}


//AddGraph shim
func (shim *EditDirectClient) AddGraph(ctx context.Context, in *GraphID, opts ...grpc.CallOption) (*EditResult, error) {
	return shim.server.AddGraph(ctx, in)
}

//DeleteGraph shim
func (shim *EditDirectClient) DeleteGraph(ctx context.Context, in *GraphID, opts ...grpc.CallOption) (*EditResult, error) {
	return shim.server.DeleteGraph(ctx, in)
}

//DeleteVertex shim
func (shim *EditDirectClient) DeleteVertex(ctx context.Context, in *ElementID, opts ...grpc.CallOption) (*EditResult, error) {
	return shim.server.DeleteVertex(ctx, in)
}

//DeleteEdge shim
func (shim *EditDirectClient) DeleteEdge(ctx context.Context, in *ElementID, opts ...grpc.CallOption) (*EditResult, error) {
	return shim.server.DeleteEdge(ctx, in)
}

//AddIndex shim
func (shim *EditDirectClient) AddIndex(ctx context.Context, in *IndexID, opts ...grpc.CallOption) (*EditResult, error) {
	return shim.server.AddIndex(ctx, in)
}

//DeleteIndex shim
func (shim *EditDirectClient) DeleteIndex(ctx context.Context, in *IndexID, opts ...grpc.CallOption) (*EditResult, error) {
	return shim.server.DeleteIndex(ctx, in)
}

//AddSchema shim
func (shim *EditDirectClient) AddSchema(ctx context.Context, in *Graph, opts ...grpc.CallOption) (*EditResult, error) {
	return shim.server.AddSchema(ctx, in)
}
