package jobstorage

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"path/filepath"
	"time"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	opensearch "github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

type OpenSearchStorage struct {
	client *opensearchapi.Client
}

var OS_INDEX_LIST string = "gripql-job-tables"

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

	resp, err := client.Indices.Exists([]string{OS_INDEX_LIST})
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

}

func (os *OpenSearchStorage) Spool(graph string, stream *Stream) (string, error) {

	tableName := fmt.Sprintf("grip-table-%10d", rand.Int())

	_, err := os.client.Indices.Create(context.Background(), opensearchapi.IndicesCreateReq{Index: tableName})
	if err != nil {
		return "", err
	}

	cs, _ := TraversalChecksum(stream.Query)
	job := &Job{
		Status:        gripql.JobStatus{Query: stream.Query, Id: tableName, Graph: graph, Timestamp: time.Now().Format(time.RFC3339)},
		DataType:      stream.DataType,
		MarkTypes:     stream.MarkTypes,
		StepChecksums: cs,
	}

	//fs.jobs.Store(jobKey(graph, tableName), job)

	ctx := context.Background()

	tbStream := MarshalStream(stream.Pipe, 4) //TODO: make worker count configurable
	go func() {
		job.Status.State = gripql.JobState_RUNNING
		log.Infof("Starting Job: %#v", job)
		//TODO: this could probably be accelerated using bulk insert
		for i := range tbStream {
			os.client.Index(ctx,
				opensearchapi.IndexReq{
					Index: tableName,
					Body:  bytes.NewReader(i),
				})
			job.Status.Count += 1
		}
		statusPath := filepath.Join(spoolDir, "status")
		statusFile, err := os.Create(statusPath)
		if err == nil {
			defer statusFile.Close()
			job.Status.State = gripql.JobState_COMPLETE
			out, err := json.Marshal(job)
			if err == nil {
				statusFile.Write([]byte(fmt.Sprintf("%s\n", out)))
			}
			log.Infof("Job Done: %s (%d results)", jobName, job.Status.Count)
		} else {
			job.Status.State = gripql.JobState_ERROR
			log.Infof("Job Error: %s %s", jobName, err)
		}
	}()
	return jobName, nil

}

func (os *OpenSearchStorage) Stream(ctx context.Context, graph, id string) (*Stream, error) {

}

func (os *OpenSearchStorage) Delete(graph, id string) error {

}

func (os *OpenSearchStorage) Status(graph, id string) (*gripql.JobStatus, error) {

}
