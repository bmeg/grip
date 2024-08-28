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

	"github.com/bmeg/grip/gdbi"
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

type OpenSearchJob struct {
	Index         string
	Graph         string
	Status        gripql.JobStatus
	DataType      gdbi.DataType
	MarkTypes     map[string]gdbi.DataType
	StepChecksums []string
}

func NewOpenSearchStorage(addr string, username, password string) (JobStorage, error) {
	log.Infof("OpenSearch Job Storage: %s %s", addr, username)
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
		if resp.StatusCode == 404 {
			//Create the job list index if it doesn't exist
			_, err := client.Indices.Create(context.Background(), opensearchapi.IndicesCreateReq{Index: OS_INDEX_LIST})
			if err != nil {
				return nil, err
			}
		} else {
			log.Errorf("Contact error: %s %#v", err, resp)
			return nil, err
		}
	}
	return &OpenSearchStorage{
		client: client,
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
				Params: opensearchapi.SearchParams{
					Query: fmt.Sprintf(`Graph: "%s"`, graph),
				},
			},
		)
		if err == nil {
			for _, i := range searchResp.Hits.Hits {
				d := map[string]any{}
				json.Unmarshal(i.Source, &d)
				//log.Infof("Search response: %#v", d)
				if x, ok := d["Index"]; ok {
					cout <- x.(string)
				}
			}
		} else {
			log.Errorf("JobList error: %s", err)
		}
	}()
	return cout, nil
}

func (os *OpenSearchStorage) Search(graph string, Query []*gripql.GraphStatement) (chan *gripql.JobStatus, error) {
	return nil, nil
}

func (os *OpenSearchStorage) putJob(id string, job *OpenSearchJob) error {
	resp, err := os.client.Index(context.Background(), opensearchapi.IndexReq{
		Index:      OS_INDEX_LIST,
		DocumentID: id,
		Body:       opensearchutil.NewJSONReader(job),
	})
	log.Infof("Job Index resp: %#v %s", resp, err)
	return err
}

func (os *OpenSearchStorage) Spool(graph string, stream *Stream) (string, error) {
	jobName := fmt.Sprintf("grip-%10d", rand.Int())
	jobID := graph + "-" + jobName

	cs, _ := TraversalChecksum(stream.Query)
	job := &OpenSearchJob{
		Index:         jobID,
		Graph:         graph,
		Status:        gripql.JobStatus{Query: stream.Query, Id: jobName, Graph: graph, Timestamp: time.Now().Format(time.RFC3339)},
		DataType:      stream.DataType,
		MarkTypes:     stream.MarkTypes,
		StepChecksums: cs,
	}
	err := os.putJob(jobID, job)
	if err != nil {
		return "", err
	}
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
