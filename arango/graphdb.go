package arango

import (
	"context"
	"encoding/base64"
	"fmt"
	"maps"
	"strings"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/timestamp"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/arangodb/go-driver/v2/arangodb"
	"github.com/arangodb/go-driver/v2/connection"
)

// Config describes the configuration for the ArangoDB driver.
type Config struct {
	URL             string
	Username        string
	Password        string
	BatchSize       int
	DBName          string
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
	conf   Config
	client arangodb.Client
	db     arangodb.Database
	ts     *timestamp.Timestamp
}

// NewGraphDB validates config and creates an Arango GraphDB scaffold.
func NewGraphDB(conf Config) (gdbi.GraphDB, error) {
	conf.SetDefaults()

	endpoint := connection.NewRoundRobinEndpoints([]string{conf.URL})
	conn := connection.NewHttp2Connection(connection.DefaultHTTP2ConfigurationWrapper(endpoint /*InsecureSkipVerify*/, true))

	// Add authentication
	auth := connection.NewBasicAuth(conf.Username, conf.Password)
	err := conn.SetAuthentication(auth)
	if err != nil {
		return nil, fmt.Errorf("failed to set authentication: %w", err)
	}

	// Create a client
	client := arangodb.NewClient(conn)

	ctx := context.Background()
	var db arangodb.Database
	dbExists, err := client.DatabaseExists(ctx, conf.DBName)
	if err != nil {
		return nil, err
	}
	if dbExists {
		db, err = client.GetDatabase(ctx, conf.DBName, nil)
		if err != nil {
			return nil, err
		}
	} else {
		db, err = client.CreateDatabase(ctx, conf.DBName, nil)
		if err != nil {
			return nil, err
		}
	}

	gDB := &GraphDB{
		conf:   conf,
		client: client,
		db:     db,
		ts:     nil,
	}
	ts := timestamp.NewTimestamp()
	gDB.ts = &ts

	for _, g := range gDB.ListGraphs() {
		gDB.ts.Touch(g)
	}

	return gDB, nil
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

func vertexCollection(graphName string) string {
	return graphName + "_nodes"
}

func edgeCollection(graphName string) string {
	return graphName + "_edges"
}

func documentHandle(collectionName, key string) string {
	return collectionName + "/" + encodeDocumentKey(key)
}

func stripDocumentHandle(value string) string {
	if idx := strings.LastIndex(value, "/"); idx >= 0 && idx+1 < len(value) {
		return decodeDocumentKey(value[idx+1:])
	}
	return decodeDocumentKey(value)
}

func encodeDocumentKey(key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(key))
}

func decodeDocumentKey(key string) string {
	data, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil {
		return key
	}
	return string(data)
}

func packVertex(v *gdbi.Vertex) map[string]any {
	out := map[string]any{}
	if v.Data != nil {
		maps.Copy(out, v.Data)
	}
	out[fieldID] = encodeDocumentKey(v.ID)
	out[fieldLabel] = v.Label
	return out
}

func packEdge(graphName string, edge *gdbi.Edge) map[string]any {
	out := map[string]any{}
	if edge.Data != nil {
		maps.Copy(out, edge.Data)
	}
	out[fieldID] = encodeDocumentKey(edge.ID)
	out[fieldLabel] = edge.Label
	out[fieldFrom] = documentHandle(vertexCollection(graphName), edge.From)
	out[fieldTo] = documentHandle(vertexCollection(graphName), edge.To)
	return out
}

func unpackVertex(doc map[string]any) *gdbi.Vertex {
	out := &gdbi.Vertex{Data: map[string]any{}, Loaded: true}
	if id, ok := doc[fieldID].(string); ok {
		out.ID = decodeDocumentKey(id)
	}
	if label, ok := doc[fieldLabel].(string); ok {
		out.Label = label
	}
	for key, value := range doc {
		if key != fieldID && key != fieldLabel && key != "_id" && key != "_rev" {
			out.Data[key] = value
		}
	}
	return out
}

func unpackEdge(doc map[string]any) *gdbi.Edge {
	out := &gdbi.Edge{Data: map[string]any{}, Loaded: true}
	if id, ok := doc[fieldID].(string); ok {
		out.ID = decodeDocumentKey(id)
	}
	if label, ok := doc[fieldLabel].(string); ok {
		out.Label = label
	}
	if from, ok := doc[fieldFrom].(string); ok {
		out.From = stripDocumentHandle(from)
	}
	if to, ok := doc[fieldTo].(string); ok {
		out.To = stripDocumentHandle(to)
	}
	for key, value := range doc {
		if key != fieldID && key != fieldLabel && key != fieldFrom && key != fieldTo && key != "_id" && key != "_rev" {
			out.Data[key] = value
		}
	}
	return out
}

func (db *GraphDB) queryMaps(query string, bindVars map[string]any) ([]map[string]any, error) {
	cursor, err := db.db.Query(context.Background(), query, &arangodb.QueryOptions{
		BindVars:  bindVars,
		BatchSize: db.conf.BatchSize,
	})
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	out := []map[string]any{}
	for cursor.HasMore() {
		row := map[string]any{}
		if _, err := cursor.ReadDocument(context.Background(), &row); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

func (db *GraphDB) execQuery(query string, bindVars map[string]any) error {
	cursor, err := db.db.Query(context.Background(), query, &arangodb.QueryOptions{
		BindVars:  bindVars,
		BatchSize: db.conf.BatchSize,
	})
	if err != nil {
		return err
	}
	defer cursor.Close()

	for cursor.HasMore() {
		var ignored any
		if _, err := cursor.ReadDocument(context.Background(), &ignored); err != nil {
			return err
		}
	}
	return nil
}

// AddGraph will create a graph in ArangoDB in a follow-up change.
func (db *GraphDB) AddGraph(graphName string) error {
	if err := gripql.ValidateGraphName(graphName); err != nil {
		return err
	}

	edgeDefinition := arangodb.EdgeDefinition{
		Collection: graphName + "_edges",           // Edge collection name
		From:       []string{graphName + "_nodes"}, // Source vertex collections
		To:         []string{graphName + "_nodes"}, // Target vertex collections
	}

	_, err := db.db.CreateGraph(context.Background(), graphName,
		&arangodb.GraphDefinition{
			EdgeDefinitions: []arangodb.EdgeDefinition{edgeDefinition},
		},
		nil)

	if err != nil {
		return err
	}

	if db.ts != nil {
		db.ts.Touch(graphName)
	}
	return nil
}

// DeleteGraph will drop a graph in ArangoDB in a follow-up change.
func (db *GraphDB) DeleteGraph(graph string) error {
	if err := gripql.ValidateGraphName(graph); err != nil {
		return err
	}
	gr, err := db.db.Graph(context.Background(), graph, nil)
	if err != nil {
		return err
	}
	if err := gr.Remove(context.Background(), &arangodb.RemoveGraphOptions{DropCollections: true}); err != nil {
		return err
	}
	if db.ts != nil {
		db.ts.Touch(graph)
	}
	return nil
}

// ListGraphs will list graphs from ArangoDB in a follow-up change.
func (db *GraphDB) ListGraphs() []string {
	reader, err := db.db.Graphs(context.Background())
	if err != nil {
		return []string{}
	}
	out := []string{}
	for {
		graph, err := reader.Read()
		if err != nil {
			break
		}
		out = append(out, graph.Name())
	}
	return out
}

// Graph will return a graph handle in a follow-up change.
func (db *GraphDB) Graph(graphID string) (gdbi.GraphInterface, error) {
	if err := gripql.ValidateGraphName(graphID); err != nil {
		return nil, err
	}

	graph, err := db.db.Graph(context.Background(), graphID, nil)
	if err != nil {
		return nil, err
	}
	vertexCol, err := graph.VertexCollection(context.Background(), vertexCollection(graphID))
	if err != nil {
		return nil, err
	}
	vertexCollectionHandle, err := db.db.GetCollection(context.Background(), vertexCollection(graphID), nil)
	if err != nil {
		return nil, err
	}
	edgeCol, err := graph.EdgeDefinition(context.Background(), edgeCollection(graphID))
	if err != nil {
		return nil, err
	}

	return &Graph{ar: db, graph: graph, graphName: graphID, vertexCol: vertexCollectionHandle, vertexCollection: vertexCol, edgeCollection: edgeCol, ts: db.ts, batchSize: db.conf.BatchSize}, nil
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
