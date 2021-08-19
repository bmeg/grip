package server

import (
	"context"

	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jobstorage"
	"github.com/bmeg/grip/log"
)

func (server *GripServer) Submit(ctx context.Context, query *gripql.GraphQuery) (*gripql.QueryJob, error) {

	gdb, err := server.getGraphDB(query.Graph)
	if err != nil {
		return nil, err
	}
	graph, err := gdb.Graph(query.Graph)
	if err != nil {
		return nil, err
	}
	compiler := graph.Compiler()
	pipe, err := compiler.Compile(query.Query, nil)
	if err != nil {
		return nil, err
	}
	//should these get stored?
	dataType := pipe.DataType()
	markTypes := pipe.MarkTypes()
	man := engine.NewManager(server.conf.Server.WorkDir)
	bufsize := 5000 //make this configurable?

	res := pipeline.Start(context.Background(), pipe, man, bufsize, nil, nil)
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
	}
	return nil
}

func (server *GripServer) SearchJobs(query *gripql.GraphQuery, srv gripql.Job_SearchJobsServer) error {
	stream, err := server.jStorage.Search(query.Graph, query.Query)
	if err != nil {
		return err
	}
	for i := range stream {
		srv.Send(i)
	}
	return nil
}

func (server *GripServer) ViewJob(job *gripql.QueryJob, srv gripql.Job_ViewJobServer) error {
	stream, err := server.jStorage.Stream(context.Background(), job.Graph, job.Id)
	if err != nil {
		return nil
	}
	gdb, err := server.getGraphDB(job.Graph)
	if err != nil {
		return err
	}
	graph, err := gdb.Graph(job.Graph)
	for o := range stream.Pipe {
		res := pipeline.Convert(graph, stream.DataType, stream.MarkTypes, o)
		srv.Send(res)
	}
	return nil
}

func (server *GripServer) ResumeJob(query *gripql.ExtendQuery, srv gripql.Job_ResumeJobServer) error {

	gdb, err := server.getGraphDB(query.Graph)
	if err != nil {
		return err
	}
	graph, err := gdb.Graph(query.Graph)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := server.jStorage.Stream(ctx, query.Graph, query.SrcId)
	if err != nil {
		return err
	}
	compiler := graph.Compiler()
	log.Infof("Compiling resume pipeline: %s", stream.DataType)
	pipe, err := compiler.Compile(query.Query, &gdbi.CompileOptions{PipelineExtension: stream.DataType, ExtensionMarkTypes: stream.MarkTypes})
	if err != nil {
		cancel()
		go func() {
			for range stream.Pipe {
			}
		}()
		return err
	}
	res := pipeline.Resume(context.Background(), pipe, server.conf.Server.WorkDir, stream.Pipe, cancel)
	for o := range res {
		srv.Send(o)
	}
	return nil
}
