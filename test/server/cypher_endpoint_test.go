package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/server"
	"github.com/bmeg/grip/util/duration"
	"github.com/bmeg/grip/util/rpc"
	"google.golang.org/protobuf/types/known/structpb"
)

type cypherQueryCase struct {
	name           string
	query          string
	expectStatus   int
	expectRows     int
	expectContains []string
	expectAbsent   []string
	expectMinLines int
	validate       func(t *testing.T, rows []map[string]any, body string)
}

func TestCypherEndpointRunner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	conf := config.DefaultConfig()
	conf.AddBadgerDefault()
	config.TestifyConfig(conf)

	t.Cleanup(func() {
		_ = os.RemoveAll(conf.Server.WorkDir)
		if conf.Default != "" {
			if d, ok := conf.Drivers[conf.Default]; ok && d.Badger != nil {
				_ = os.RemoveAll(*d.Badger)
			}
		}
	})

	srv, err := server.NewGripServer(conf, "./", nil)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	go srv.Serve(ctx)

	if err := waitForHTTP(conf.Server.HTTPPort, 10*time.Second); err != nil {
		t.Fatalf("server did not become ready: %v", err)
	}

	cli, err := gripql.Connect(rpc.Config{ServerAddress: conf.Server.RPCAddress(), Timeout: duration.Duration(5 * time.Second)}, true)
	if err != nil {
		t.Fatalf("failed to create GRPC client: %v", err)
	}

	graph := "cypher-test"
	if err := seedCypherGraph(cli, graph); err != nil {
		t.Fatalf("failed to seed graph: %v", err)
	}

	cases := []cypherQueryCase{
		{
			name:           "match by property",
			query:          "MATCH (n:Person {name: 'Bob'}) RETURN n",
			expectStatus:   http.StatusOK,
			expectRows:     1,
			expectContains: []string{"Bob"},
			expectMinLines: 1,
		},
		{
			name:           "edge traversal projection",
			query:          "MATCH (n:Person {name: 'Bob'})-[:FRIEND]->(friend) RETURN friend.name AS friendName",
			expectStatus:   http.StatusOK,
			expectRows:     1,
			expectContains: []string{"Alice"},
			expectAbsent:   []string{"Carol"},
			expectMinLines: 1,
		},
		{
			name:           "limit establishes deterministic row count",
			query:          "MATCH (n:Person) RETURN n LIMIT 2",
			expectStatus:   http.StatusOK,
			expectRows:     2,
			expectMinLines: 2,
		},
		{
			name:           "multi hop traversal reaches friend of friend",
			query:          "MATCH (n:Person {name: 'Bob'})-[:FRIEND]->()-[:FRIEND]->(fof) RETURN fof.name AS fofName",
			expectStatus:   http.StatusOK,
			expectRows:     1,
			expectContains: []string{"Carol"},
			expectAbsent:   []string{"Alice", "Bob"},
			expectMinLines: 1,
		},
		{
			name:           "no matches returns empty body",
			query:          "MATCH (n:Person {name: 'Nobody'}) RETURN n",
			expectStatus:   http.StatusOK,
			expectRows:     0,
			expectAbsent:   []string{"Bob", "Alice", "Carol"},
			expectMinLines: 0,
		},
		{
			name:           "aggregation query currently unsupported",
			query:          "MATCH (n:Person) RETURN count(n)",
			expectStatus:   http.StatusInternalServerError,
			expectRows:     -1,
			expectContains: []string{"failed to parse query"},
			expectMinLines: 1,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			resp, body, err := postCypherQuery(conf.Server.HTTPPort, graph, tc.query)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}

			expectedStatus := tc.expectStatus
			if expectedStatus == 0 {
				expectedStatus = http.StatusOK
			}
			if resp.StatusCode != expectedStatus {
				t.Fatalf("unexpected status code %d: %s", resp.StatusCode, body)
			}

			lines := splitNonEmptyLines(body)
			if tc.expectRows >= 0 && len(lines) != tc.expectRows {
				t.Fatalf("expected exactly %d result line(s), got %d; body: %s", tc.expectRows, len(lines), body)
			}
			if len(lines) < tc.expectMinLines {
				t.Fatalf("expected at least %d result line(s), got %d; body: %s", tc.expectMinLines, len(lines), body)
			}

			rows := []map[string]any{}
			if resp.StatusCode == http.StatusOK {
				rows, err = parseResponseRows(lines)
				if err != nil {
					t.Fatalf("response parse failure: %v", err)
				}
			}

			for _, expected := range tc.expectContains {
				if !strings.Contains(body, expected) {
					t.Fatalf("response body missing expected token %q; body: %s", expected, body)
				}
			}

			for _, unexpected := range tc.expectAbsent {
				if strings.Contains(body, unexpected) {
					t.Fatalf("response body unexpectedly contains %q; body: %s", unexpected, body)
				}
			}

			if tc.validate != nil {
				tc.validate(t, rows, body)
			}
		})
	}
}

func waitForHTTP(port string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	url := fmt.Sprintf("http://localhost:%s/v1/graph", port)

	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("timed out waiting for %s", url)
}

func seedCypherGraph(cli gripql.Client, graph string) error {
	if err := cli.AddGraph(graph); err != nil {
		return err
	}

	bobData, err := structpb.NewStruct(map[string]any{"name": "Bob", "age": 41})
	if err != nil {
		return err
	}
	aliceData, err := structpb.NewStruct(map[string]any{"name": "Alice", "age": 33})
	if err != nil {
		return err
	}
	carolData, err := structpb.NewStruct(map[string]any{"name": "Carol", "age": 29})
	if err != nil {
		return err
	}

	if err := cli.AddVertex(graph, &gripql.Vertex{Id: "bob", Label: "Person", Data: bobData}); err != nil {
		return err
	}
	if err := cli.AddVertex(graph, &gripql.Vertex{Id: "alice", Label: "Person", Data: aliceData}); err != nil {
		return err
	}
	if err := cli.AddVertex(graph, &gripql.Vertex{Id: "carol", Label: "Person", Data: carolData}); err != nil {
		return err
	}

	if err := cli.AddEdge(graph, &gripql.Edge{Id: "e1", From: "bob", To: "alice", Label: "FRIEND"}); err != nil {
		return err
	}
	if err := cli.AddEdge(graph, &gripql.Edge{Id: "e2", From: "alice", To: "carol", Label: "FRIEND"}); err != nil {
		return err
	}

	return nil
}

func postCypherQuery(port, graph, query string) (*http.Response, string, error) {
	url := fmt.Sprintf("http://localhost:%s/cypher/%s", port, graph)
	req, err := http.NewRequest("POST", url, bytes.NewBufferString(query))
	if err != nil {
		return nil, "", err
	}

	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}

	return resp, string(body), nil
}

func splitNonEmptyLines(s string) []string {
	lines := strings.Split(s, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func parseResponseRows(lines []string) ([]map[string]any, error) {
	rows := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		if !json.Valid([]byte(line)) {
			return nil, fmt.Errorf("response line is not valid JSON: %q", line)
		}

		row := map[string]any{}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("cannot parse response line %q: %w", line, err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}
