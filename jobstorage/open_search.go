package jobstorage

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"

	"github.com/bmeg/grip/gripql"
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

func (os *OpenSearchStorage) Spool(graph string, stream *Stream) (string, error) {
	tableName := fmt.Sprintf("grip-table-%10d", rand.Int())

	return tableName, nil

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
