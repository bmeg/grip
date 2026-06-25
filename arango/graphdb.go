package arango

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/timestamp"
	"google.golang.org/protobuf/types/known/structpb"
)

// Config describes the configuration for the ArangoDB driver.
type Config struct {
	URL             string
	DBName          string
	Username        string
	Password        string
	BatchSize       int
	UseCorePipeline bool
}

// SetDefaults applies default values for unset configuration fields.
func (c *Config) SetDefaults() {
	if c.URL == "" {
		c.URL = "http://localhost:8529"
	}
	if c.DBName == "" {
		c.DBName = "gripdb"
	}
	if c.BatchSize == 0 {
		c.BatchSize = 1000
	}
}

// GraphDB is a scaffold for the Arango-backed GraphDB implementation.
type GraphDB struct {
	conf     Config
	database string
	http     *http.Client
	ts       *timestamp.Timestamp
}

// NewGraphDB validates config and creates an Arango GraphDB scaffold.
func NewGraphDB(conf Config) (gdbi.GraphDB, error) {
	conf.SetDefaults()

	database := strings.ToLower(conf.DBName)
	if err := gripql.ValidateGraphName(database); err != nil {
		return nil, fmt.Errorf("invalid database name: %v", err)
	}

	db := &GraphDB{
		conf:     conf,
		database: database,
		http:     &http.Client{Timeout: 30 * time.Second},
		ts:       nil,
	}
	ts := timestamp.NewTimestamp()
	db.ts = &ts

	if err := db.ensureDatabase(); err != nil {
		return nil, err
	}
	for _, g := range db.ListGraphs() {
		db.ts.Touch(g)
	}

	return db, nil
}

type arangoError struct {
	HasError     bool   `json:"error"`
	Code         int    `json:"code"`
	ErrorNum     int    `json:"errorNum"`
	ErrorMessage string `json:"errorMessage"`
}

func (e arangoError) Error() string {
	if e.ErrorMessage != "" {
		return fmt.Sprintf("arango error (code=%d errorNum=%d): %s", e.Code, e.ErrorNum, e.ErrorMessage)
	}
	return fmt.Sprintf("arango error (code=%d errorNum=%d)", e.Code, e.ErrorNum)
}

func (db *GraphDB) endpoint(path string) string {
	return strings.TrimRight(db.conf.URL, "/") + path
}

func (db *GraphDB) doJSON(method, path string, in any, out any) error {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("marshal request body: %w", err)
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, db.endpoint(path), body)
	if err != nil {
		return fmt.Errorf("building request %s %s: %w", method, path, err)
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if db.conf.Username != "" || db.conf.Password != "" {
		req.SetBasicAuth(db.conf.Username, db.conf.Password)
	}

	resp, err := db.http.Do(req)
	if err != nil {
		return fmt.Errorf("executing request %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body %s %s: %w", method, path, err)
	}

	if len(respData) > 0 {
		apiErr := arangoError{}
		if json.Unmarshal(respData, &apiErr) == nil && apiErr.HasError {
			return apiErr
		}
	}

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("arango request %s %s failed: HTTP %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(respData)))
	}

	if out != nil && len(respData) > 0 {
		if err := json.Unmarshal(respData, out); err != nil {
			return fmt.Errorf("decoding response body %s %s: %w", method, path, err)
		}
	}

	return nil
}

func isArangoErrorNum(err error, nums ...int) bool {
	apiErr := arangoError{}
	if !matchesArangoError(err, &apiErr) {
		return false
	}
	for _, n := range nums {
		if apiErr.ErrorNum == n {
			return true
		}
	}
	return false
}

func matchesArangoError(err error, target *arangoError) bool {
	if err == nil {
		return false
	}
	parsed, ok := err.(arangoError)
	if ok {
		*target = parsed
		return true
	}
	return false
}

func (db *GraphDB) ensureDatabase() error {
	err := db.doJSON(http.MethodPost, "/_db/_system/_api/database", map[string]string{"name": db.database}, nil)
	if err != nil && !isArangoErrorNum(err, 1207) {
		return fmt.Errorf("ensuring arango database %s: %w", db.database, err)
	}
	err = db.createCollection("graphs")
	if err != nil {
		return fmt.Errorf("ensuring graphs collection: %w", err)
	}
	return nil
}

func (db *GraphDB) aqlQuery(path string, payload map[string]any, out any) error {
	return db.doJSON(http.MethodPost, path, payload, out)
}

func (db *GraphDB) queryMaps(query string, bindVars map[string]any) ([]map[string]any, error) {
	resp := struct {
		Result []map[string]any `json:"result"`
	}{Result: []map[string]any{}}
	if err := db.aqlQuery(
		fmt.Sprintf("/_db/%s/_api/cursor", url.PathEscape(db.database)),
		map[string]any{"query": query, "bindVars": bindVars},
		&resp,
	); err != nil {
		return nil, err
	}
	return resp.Result, nil
}

func (db *GraphDB) execAQL(query string, bindVars map[string]any) error {
	return db.aqlQuery(
		fmt.Sprintf("/_db/%s/_api/cursor", url.PathEscape(db.database)),
		map[string]any{"query": query, "bindVars": bindVars},
		nil,
	)
}

func (db *GraphDB) createCollection(name string) error {
	err := db.doJSON(http.MethodPost, fmt.Sprintf("/_db/%s/_api/collection", url.PathEscape(db.database)), map[string]any{"name": name}, nil)
	if err != nil && !isArangoErrorNum(err, 1207) {
		return err
	}
	return nil
}

func (db *GraphDB) dropCollection(name string) error {
	err := db.doJSON(http.MethodDelete, fmt.Sprintf("/_db/%s/_api/collection/%s", url.PathEscape(db.database), url.PathEscape(name)), nil, nil)
	if err != nil && !isArangoErrorNum(err, 1203) {
		return err
	}
	return nil
}

func (db *GraphDB) addGraphRecord(graph string) error {
	err := db.doJSON(http.MethodPost, fmt.Sprintf("/_db/%s/_api/document/graphs", url.PathEscape(db.database)), map[string]any{"_key": graph}, nil)
	if err != nil && !isArangoErrorNum(err, 1210) {
		return err
	}
	return nil
}

func (db *GraphDB) removeGraphRecord(graph string) error {
	err := db.doJSON(http.MethodDelete, fmt.Sprintf("/_db/%s/_api/document/graphs/%s", url.PathEscape(db.database), url.PathEscape(graph)), nil, nil)
	if err != nil && !isArangoErrorNum(err, 1202) {
		return err
	}
	return nil
}

func vertexCollection(graph string) string {
	return fmt.Sprintf("%s_vertices", graph)
}

func edgeCollection(graph string) string {
	return fmt.Sprintf("%s_edges", graph)
}

// AddGraph will create a graph in ArangoDB in a follow-up change.
func (db *GraphDB) AddGraph(graph string) error {
	if err := gripql.ValidateGraphName(graph); err != nil {
		return err
	}
	if err := db.ensureDatabase(); err != nil {
		return err
	}
	if err := db.createCollection(vertexCollection(graph)); err != nil {
		return fmt.Errorf("AddGraph: creating vertex collection for %s: %w", graph, err)
	}
	if err := db.createCollection(edgeCollection(graph)); err != nil {
		return fmt.Errorf("AddGraph: creating edge collection for %s: %w", graph, err)
	}
	if err := db.addGraphRecord(graph); err != nil {
		return fmt.Errorf("AddGraph: storing graph record for %s: %w", graph, err)
	}
	if db.ts != nil {
		db.ts.Touch(graph)
	}
	return nil
}

// DeleteGraph will drop a graph in ArangoDB in a follow-up change.
func (db *GraphDB) DeleteGraph(graph string) error {
	if err := gripql.ValidateGraphName(graph); err != nil {
		return err
	}
	if err := db.dropCollection(vertexCollection(graph)); err != nil {
		return fmt.Errorf("DeleteGraph: dropping vertex collection for %s: %w", graph, err)
	}
	if err := db.dropCollection(edgeCollection(graph)); err != nil {
		return fmt.Errorf("DeleteGraph: dropping edge collection for %s: %w", graph, err)
	}
	if err := db.removeGraphRecord(graph); err != nil {
		return fmt.Errorf("DeleteGraph: deleting graph %s metadata: %w", graph, err)
	}
	if db.ts != nil {
		db.ts.Touch(graph)
	}
	return nil
}

// ListGraphs will list graphs from ArangoDB in a follow-up change.
func (db *GraphDB) ListGraphs() []string {
	if err := db.ensureDatabase(); err != nil {
		return nil
	}

	result := struct {
		Result []string `json:"result"`
	}{Result: []string{}}
	err := db.doJSON(
		http.MethodPost,
		fmt.Sprintf("/_db/%s/_api/cursor", url.PathEscape(db.database)),
		map[string]any{
			"query":    "FOR g IN @@c RETURN g._key",
			"bindVars": map[string]any{"@c": "graphs"},
		},
		&result,
	)
	if err != nil {
		return nil
	}

	return result.Result
}

// Graph will return a graph handle in a follow-up change.
func (db *GraphDB) Graph(graphID string) (gdbi.GraphInterface, error) {
	if err := gripql.ValidateGraphName(graphID); err != nil {
		return nil, err
	}
	found := false
	for _, g := range db.ListGraphs() {
		if g == graphID {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("graph '%s' was not found", graphID)
	}
	return &Graph{ar: db, ts: db.ts, graph: graphID, batchSize: db.conf.BatchSize}, nil
}

func (db *GraphDB) BuildSchema(ctx context.Context, graphID string, sampleN uint32, random bool) (*gripql.Graph, error) {
	gi, err := db.Graph(graphID)
	if err != nil {
		return nil, err
	}
	g := gi.(*Graph)

	vLabels, err := g.ListVertexLabels()
	if err != nil {
		return nil, err
	}
	eLabels, err := g.ListEdgeLabels()
	if err != nil {
		return nil, err
	}

	vSchema := make([]*gripql.Vertex, 0, len(vLabels))
	for _, label := range vLabels {
		d, _ := structpb.NewStruct(map[string]any{})
		vSchema = append(vSchema, &gripql.Vertex{Id: label, Label: "Vertex", Data: d})
	}

	eSchema := make([]*gripql.Edge, 0, len(eLabels))
	for _, label := range eLabels {
		eSchema = append(eSchema, &gripql.Edge{Id: label, Label: label})
	}

	return &gripql.Graph{Graph: graphID, Vertices: vSchema, Edges: eSchema}, nil
}

// Close closes resources used by the Arango driver.
func (db *GraphDB) Close() error {
	return nil
}
