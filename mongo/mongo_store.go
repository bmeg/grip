package mongo

import (
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/timestamp"

	//"github.com/bmeg/golib/timing"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
)

// NewArachne creates a new ArachneInterface using the given
// mongo server url and database name
func NewArachne(url string, database string) gdbi.ArachneInterface {
	session, err := mgo.Dial(url)
	if err != nil {
		log.Printf("%s", err)
	}
	db := session.DB(database)
	ts := timestamp.NewTimestamp()
	a := &Arachne{db: db, ts: &ts}
	for _, i := range a.GetGraphs() {
		a.ts.Touch(i)
	}
	return a
}

// Arachne is the base driver that manages multiple graphs in mongo
type Arachne struct {
	db *mgo.Database
	ts *timestamp.Timestamp
}

// Graph is the tnterface to a single graph
type Graph struct {
	vertices *mgo.Collection
	edges    *mgo.Collection
	ts       *timestamp.Timestamp //BUG: This timestamp implementation doesn't work againt multiple mongo clients
	graph    string
}

// AddGraph creates a new graph named `graph`
func (ma *Arachne) AddGraph(graph string) error {
	graphs := ma.db.C(fmt.Sprintf("graphs"))
	graphs.Insert(map[string]string{"_id": graph})

	//v := ma.db.C(fmt.Sprintf("%s_vertices", graph))
	e := ma.db.C(fmt.Sprintf("%s_edges", graph))
	e.EnsureIndex(mgo.Index{Key: []string{"$hashed:from"}})
	e.EnsureIndex(mgo.Index{Key: []string{"$hashed:to"}})
	e.EnsureIndex(mgo.Index{Key: []string{"$hashed:label"}})

	v := ma.db.C(fmt.Sprintf("%s_vertices", graph))
	v.EnsureIndex(mgo.Index{Key: []string{"$hashed:label"}})

	ma.ts.Touch(graph)
	return nil
}

// Close the connection
func (ma *Arachne) Close() {
	ma.db.Logout()
}

// DeleteGraph deletes `graph`
func (ma *Arachne) DeleteGraph(graph string) error {
	g := ma.db.C(fmt.Sprintf("graphs"))
	v := ma.db.C(fmt.Sprintf("%s_vertices", graph))
	e := ma.db.C(fmt.Sprintf("%s_edges", graph))
	v.DropCollection()
	e.DropCollection()
	g.RemoveId(graph)
	ma.ts.Touch(graph)
	return nil
}

// GetGraphs lists the graphs managed by this driver
func (ma *Arachne) GetGraphs() []string {
	out := make([]string, 0, 100)
	g := ma.db.C(fmt.Sprintf("graphs"))

	iter := g.Find(nil).Iter()
	defer iter.Close()
	result := map[string]interface{}{}
	for iter.Next(&result) {
		out = append(out, result["_id"].(string))
	}

	return out
}

// Graph obtains the gdbi.DBI for a particular graph
func (ma *Arachne) Graph(graph string) gdbi.DBI {
	return &Graph{
		vertices: ma.db.C(fmt.Sprintf("%s_vertices", graph)),
		edges:    ma.db.C(fmt.Sprintf("%s_edges", graph)),
		graph:    graph,
		ts:       ma.ts,
	}
}

// Query creates a QueryInterface for Graph graph
func (ma *Arachne) Query(graph string) gdbi.QueryInterface {
	return ma.Graph(graph).Query()
}

// Query creates a QueryInterface for a particular Graph
func (mg *Graph) Query() gdbi.QueryInterface {
	return gdbi.NewPipeEngine(mg)
}

// GetEdge loads an edge given an id. It returns nil if not found
func (mg *Graph) GetEdge(id string, loadProp bool) *aql.Edge {
	log.Printf("GetEdge: %s", id)
	d := map[string]interface{}{}
	q := mg.edges.FindId(id)
	q.One(d)
	v := UnpackEdge(d)
	return &v
}

//GetTimestamp gets the timestamp of last update
func (mg *Graph) GetTimestamp() string {
	return mg.ts.Get(mg.graph)
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (mg *Graph) GetVertex(key string, load bool) *aql.Vertex {
	//log.Printf("GetVertex: %s", key)
	d := map[string]interface{}{}
	q := mg.vertices.Find(map[string]interface{}{"_id": key}).Limit(1)
	if !load {
		q = q.Select(map[string]interface{}{"_id": 1, "label": 1})
	}
	err := q.One(d)
	if err != nil {
		return nil
	}
	v := UnpackVertex(d)
	return &v
}

// SetVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (mg *Graph) SetVertex(vertex aql.Vertex) error {
	_, err := mg.vertices.UpsertId(vertex.Gid, PackVertex(vertex))
	mg.ts.Touch(mg.graph)
	return err
}

// SetEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (mg *Graph) SetEdge(edge aql.Edge) error {
	if edge.Gid != "" {
		_, err := mg.edges.UpsertId(edge.Gid, PackEdge(edge))
		mg.ts.Touch(mg.graph)
		return err
	}
	edge.Gid = bson.NewObjectId().Hex()
	err := mg.edges.Insert(PackEdge(edge))
	mg.ts.Touch(mg.graph)
	return err
}

// DelVertex deletes vertex with id `key`
func (mg *Graph) DelVertex(key string) error {
	mg.ts.Touch(mg.graph)
	return mg.vertices.RemoveId(key)
}

// DelEdge deletes edge with id `key`
func (mg *Graph) DelEdge(key string) error {
	mg.ts.Touch(mg.graph)
	return mg.edges.RemoveId(key)
}

// GetVertexList produces a channel of all edges in the graph
func (mg *Graph) GetVertexList(ctx context.Context, load bool) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)
		iter := mg.vertices.Find(nil).Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			v := UnpackVertex(result)
			o <- v
		}
	}()
	return o
}

// GetEdgeList produces a channel of all edges in the graph
func (mg *Graph) GetEdgeList(ctx context.Context, loadProp bool) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		iter := mg.edges.Find(nil).Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if _, ok := result[fieldDst]; ok {
				e := UnpackEdge(result)
				o <- e
			} else if _, ok := result[fieldBundle]; ok {
				bundle := UnpackBundle(result)
				for k, v := range bundle.Bundle {
					e := aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Data: v}
					o <- e
				}
			}
		}
	}()
	return o
}

// BatchSize controls size of batched mongo queries
//TODO: move this into driver config parameter
var BatchSize = 100

// GetVertexListByID is passed a channel of vertex ids and it produces a channel
// of vertices
func (mg *Graph) GetVertexListByID(ctx context.Context, ids chan string, load bool) chan *aql.Vertex {
	batches := make(chan []string, 100)
	go func() {
		defer close(batches)
		o := make([]string, 0, BatchSize)
		for id := range ids {
			o = append(o, id)
			if len(o) >= BatchSize {
				batches <- o
				o = make([]string, 0, BatchSize)
			}
		}
		batches <- o
	}()

	out := make(chan *aql.Vertex, 100)
	go func() {
		defer close(out)
		for batch := range batches {
			//log.Printf("Getting Batch")
			query := bson.M{"_id": bson.M{"$in": batch}}
			//log.Printf("Query: %s", query)
			q := mg.vertices.Find(query)
			if !load {
				q = q.Select(map[string]interface{}{"_id": 1, "label": 1})
			}
			iter := q.Iter()
			if iter.Err() != nil {
				log.Printf("batch err: %s", iter.Err())
			}
			defer iter.Close()
			chunk := map[string]*aql.Vertex{}
			result := map[string]interface{}{}
			for iter.Next(&result) {
				v := UnpackVertex(result)
				chunk[v.Gid] = &v
			}
			//if iter.Err() != nil {
			//	log.Printf("batch err: %s", iter.Err())
			//}

			for _, id := range batch {
				if x, ok := chunk[id]; ok {
					out <- x
				} else {
					out <- nil
				}
			}
		}
	}()

	return out
}

// GetOutList given vertex/edge `key` find vertices on outgoing edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *Graph) GetOutList(ctx context.Context, key string, load bool, edgeLabels []string) chan aql.Vertex {
	//BUG: This should respond to edge ids as well
	o := make(chan aql.Vertex, 100)
	vertexChan := make(chan string, 100)
	go func() {
		defer close(vertexChan)
		selection := map[string]interface{}{
			fieldSrc: key,
		}
		if len(edgeLabels) > 0 {
			selection[fieldLabel] = bson.M{"$in": edgeLabels}
		}
		iter := mg.edges.Find(selection).Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if _, ok := result[fieldDst]; ok {
				vertexChan <- result[fieldDst].(string)
			} else if val, ok := result[fieldBundle]; ok {
				for k := range val.(map[string]interface{}) {
					vertexChan <- k
				}
			}
		}
	}()

	go func() {
		defer close(o)
		for dst := range vertexChan {
			q := mg.vertices.FindId(dst)
			if !load {
				q = q.Select(map[string]interface{}{"_id": 1, "label": 1})
			}
			d := map[string]interface{}{}
			err := q.One(d)
			if err == nil {
				v := UnpackVertex(d)
				o <- v
			}
		}
	}()
	return o
}

// GetInList given vertex/edge `key` find vertices on incoming edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *Graph) GetInList(ctx context.Context, key string, load bool, edgeLabels []string) chan aql.Vertex {
	//BUG: this should respond to both vertex and edge ids
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)
		selection := map[string]interface{}{
			fieldDst: key,
		}
		if len(edgeLabels) > 0 {
			selection[fieldLabel] = bson.M{"$in": edgeLabels}
		}
		iter := mg.edges.Find(selection).Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			q := mg.vertices.FindId(result[fieldSrc])
			if !load {
				q = q.Select(map[string]interface{}{"_id": 1, "label": 1})
			}
			d := map[string]interface{}{}
			q.One(d)
			v := UnpackVertex(d)
			o <- v
		}
	}()
	return o
}

// GetOutEdgeList given vertex `key` find all outgoing edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *Graph) GetOutEdgeList(ctx context.Context, key string, load bool, edgeLabels []string) chan aql.Edge {
	o := make(chan aql.Edge, 1000)
	go func() {
		defer close(o)
		selection := map[string]interface{}{
			fieldSrc: key,
		}
		if len(edgeLabels) > 0 {
			selection[fieldLabel] = bson.M{"$in": edgeLabels}
		}
		iter := mg.edges.Find(selection).Iter()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			if _, ok := result[fieldDst]; ok {
				e := UnpackEdge(result)
				o <- e
			} else if _, ok := result[fieldBundle]; ok {
				bundle := UnpackBundle(result)
				for k, v := range bundle.Bundle {
					e := aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Data: v}
					o <- e
				}
			}
		}
	}()
	return o
}

// GetOutBundleList given vertex `key` find all outgoing bundles,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
// load is ignored
func (mg *Graph) GetOutBundleList(ctx context.Context, key string, load bool, edgeLabels []string) chan aql.Bundle {
	o := make(chan aql.Bundle, 1000)
	go func() {
		defer close(o)
		selection := map[string]interface{}{
			fieldSrc: key,
		}
		if len(edgeLabels) > 0 {
			selection[fieldLabel] = bson.M{"$in": edgeLabels}
		}
		iter := mg.edges.Find(selection).Iter()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			if _, ok := result[fieldBundle]; ok {
				bundle := UnpackBundle(result)
				o <- bundle
			}
		}
	}()
	return o
}

// GetInEdgeList given vertex `key` find all incoming edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *Graph) GetInEdgeList(ctx context.Context, key string, load bool, edgeLabels []string) chan aql.Edge {
	//TODO: use the load variable to filter data field from scan if possible
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		selection := map[string]interface{}{
			fieldDst: key,
		}
		if len(edgeLabels) > 0 {
			selection[fieldLabel] = bson.M{"$in": edgeLabels}
		}
		iter := mg.edges.Find(selection).Iter()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			e := UnpackEdge(result)
			o <- e
		}
	}()
	return o
}

// SetBundle adds a bundle to the graph
func (mg *Graph) SetBundle(bundle aql.Bundle) error {
	if bundle.Gid != "" {
		_, err := mg.edges.UpsertId(bundle.Gid, PackBundle(bundle))
		return err
	}
	err := mg.edges.Insert(PackBundle(bundle))
	mg.ts.Touch(mg.graph)
	return err
}

// GetBundle loads bundle of edges, given an id
// loadProp is ignored
func (mg *Graph) GetBundle(id string, loadProp bool) *aql.Bundle {
	d := map[string]interface{}{}
	q := mg.edges.FindId(id)
	q.One(d)
	v := UnpackBundle(d)
	return &v
}

// DelBundle removes a bundle of edges given an id
func (mg *Graph) DelBundle(id string) error {
	err := mg.edges.RemoveId(id)
	mg.ts.Touch(mg.graph)
	return err
}

// VertexLabelScan produces a channel of all edge ids where the edge label matches `label`
func (mg *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		selection := map[string]interface{}{
			"label": label,
		}
		iter := mg.vertices.Find(selection).Select(map[string]interface{}{"_id": 1}).Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			id := result["_id"]
			if idb, ok := id.(bson.ObjectId); ok {
				out <- idb.String()
			} else {
				out <- id.(string)
			}
		}
	}()
	return out
}

// EdgeLabelScan produces a channel of all edge ids where the edge label matches `label`
func (mg *Graph) EdgeLabelScan(ctx context.Context, label string) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		selection := map[string]interface{}{
			"label": label,
		}
		iter := mg.edges.Find(selection).Select(map[string]interface{}{"_id": 1}).Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			id := result["_id"]
			if idb, ok := id.(bson.ObjectId); ok {
				out <- idb.String()
			} else {
				out <- id.(string)
			}
		}
	}()
	return out
}
