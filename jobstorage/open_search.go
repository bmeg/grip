package jobstorage

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	opensearch "github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"
)

type OpenSearchStorage struct {
	client *opensearchapi.Client
}

var OS_INDEX_LIST string = "gripql-job-status"

func NewOpenSearchStorage(addr string, username, password string) (JobStorage, error) {
	client, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Addresses: []string{addr},
			Username:  username,
			Password:  password,
		},
	})
	if err != nil {
		return nil, err
	}

	resp, err := client.Indices.Exists(context.Background(), opensearchapi.IndicesExistsReq{Indices: []string{OS_INDEX_LIST}})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == 404 {
		//Create the job list index if it doesn't exist
		_, err := client.Indices.Create(context.Background(), opensearchapi.IndicesCreateReq{Index: OS_INDEX_LIST})
		if err != nil {
			return nil, err
		}
	}
	return &OpenSearchStorage{
		client,
	}, nil
}

func (os *OpenSearchStorage) List(graph string) (chan string, error) {
	cout := make(chan string, 5)
	go func() {
		defer close(cout)
		searchResp, err := os.client.Search(
			context.Background(),
			&opensearchapi.SearchReq{
				Indices: []string{OS_INDEX_LIST},
				Params:  opensearchapi.SearchParams{},
			},
		)
		if err == nil {
			for _, i := range searchResp.Hits.Hits {
				d := map[string]string{}
				json.Unmarshal(i.Fields, &d)
				if x, ok := d["index"]; ok {
					cout <- x
				}
			}
		}
	}()
	return cout, nil
}

func (os *OpenSearchStorage) Search(graph string, Query []*gripql.GraphStatement) (chan *gripql.JobStatus, error) {
	return nil, nil
}

func (os *OpenSearchStorage) putJob(id string, job *Job) error {
	_, err := os.client.Index(context.Background(), opensearchapi.IndexReq{
		Index:      OS_INDEX_LIST,
		DocumentID: id,
		Body:       opensearchutil.NewJSONReader(job),
	})
	return err
}

func (os *OpenSearchStorage) Spool(graph string, stream *Stream) (string, error) {
	jobName := fmt.Sprintf("grip-%10d", rand.Int())

	cs, _ := TraversalChecksum(stream.Query)
	job := &Job{
		Status:        gripql.JobStatus{Query: stream.Query, Id: jobName, Graph: graph, Timestamp: time.Now().Format(time.RFC3339)},
		DataType:      stream.DataType,
		MarkTypes:     stream.MarkTypes,
		StepChecksums: cs,
	}
	jobID := jobKey(graph, jobName)
	os.putJob(jobID, job)
	tbStream := MarshalStream(stream.Pipe, 4) //TODO: make worker count configurable
	go func() {
		job.Status.State = gripql.JobState_RUNNING
		log.Infof("Starting Job: %#v", job)
		for i := range tbStream {
			os.client.Index(context.Background(), opensearchapi.IndexReq{
				Index: jobID,
				Body:  bytes.NewReader(i)})
			job.Status.Count += 1
		}
		job.Status.State = gripql.JobState_COMPLETE
		os.putJob(jobID, job)
	}()
	return jobName, nil
}

func (os *OpenSearchStorage) Stream(ctx context.Context, graph, id string) (*Stream, error) {
	return nil, nil
}

func (os *OpenSearchStorage) Delete(graph, id string) error {
	return nil
}

func (os *OpenSearchStorage) Status(graph, id string) (*gripql.JobStatus, error) {
	return nil, nil
}
