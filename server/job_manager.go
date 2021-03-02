package server

import (
	"context"
	"log"

	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jobstorage"
)

func (server *GripServer) Job(ctx context.Context, query *gripql.GraphQuery) (*gripql.QueryJob, error) {

	gdb, err := server.getGraphDB(query.Graph)
	if err != nil {
		return nil, err
	}
	graph, err := gdb.Graph(query.Graph)
	if err != nil {
		return nil, err
	}
	compiler := graph.Compiler()
	pipe, err := compiler.Compile(query.Query)
	if err != nil {
		return nil, err
	}
	//should these get stored?
	dataType := pipe.DataType()
	markTypes := pipe.MarkTypes()
	man := engine.NewManager(server.conf.Server.WorkDir)
	bufsize := 5000 //make this configurable?
	res := pipeline.Start(ctx, pipe, man, bufsize)
	jobID, err := server.jStorage.Spool(query.Graph,
		&jobstorage.Stream{
			DataType:  dataType,
			MarkTypes: markTypes,
			Pipe:      res,
			Query:     query.Query,
		})
	return &gripql.QueryJob{
		Id:    jobID,
		Graph: query.Graph,
	}, err
}

func (server *GripServer) GetJob(ctx context.Context, job *gripql.QueryJob) (*gripql.JobStatus, error) {
	return server.jStorage.Status(job.Graph, job.Id)
}

func (server *GripServer) GetResults(job *gripql.QueryJob, srv gripql.Query_GetResultsServer) error {
	out, err := server.jStorage.Stream(job.Graph, job.Id)
	if err != nil {
		return err
	}
	for o := range out.Pipe {
		res := pipeline.Convert(out.DataType, out.MarkTypes, o)
		srv.Send(res)
	}
	return nil
}

func (server *GripServer) DeleteJob(ctx context.Context, job *gripql.QueryJob) (*gripql.JobStatus, error) {
	err := server.jStorage.Delete(job.Graph, job.Id)
	if err != nil {
		return nil, err
	}
	return &gripql.JobStatus{
		Graph: job.Graph,
		Id:    job.Id,
		State: gripql.JobState_DELETED,
	}, nil
}

func (server *GripServer) ListJobs(graph *gripql.Graph, srv gripql.Job_ListJobsServer) error {
	stream, err := server.jStorage.List(graph.Graph)
	if err != nil {
		return err
	}
	for i := range stream {
		srv.Send(&gripql.QueryJob{
			Id:    i,
			Graph: graph.Graph,
		})
		log.Printf("job id sent: %s", i)
	}
	return nil
}

func (server *GripServer) ViewJob(job *gripql.QueryJob, srv gripql.Job_ViewJobServer) error {
	stream, err := server.jStorage.Stream(job.Graph, job.Id)
	if err != nil {
		return nil
	}
	for o := range stream.Pipe {
		res := pipeline.Convert(stream.DataType, stream.MarkTypes, o)
		srv.Send(res)
	}
	return nil
}
