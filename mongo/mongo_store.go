package mongo

import (
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/timestamp"
	"github.com/vsco/mgopool"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io"
	"log"
	"strings"
)

// Mongo is the base driver that manages multiple graphs in mongo
type Mongo struct {
	url            string
	database       string
	initialSession *mgo.Session
	pool           mgopool.Pool
	ts             *timestamp.Timestamp
}

// NewMongo creates a new mongo graph database interface
func NewMongo(url string, database string) gdbi.GraphDB {
	ts := timestamp.NewTimestamp()
	session, err := mgo.Dial(url)
	if err != nil {
		log.Printf("%s", err)
	}
	b, _ := session.BuildInfo()
	if !b.VersionAtLeast(3, 2) {
		log.Printf("Requires mongo 3.2 or later")
		session.Close()
		return nil
	}
	pool := mgopool.NewLeaky(session, 3)
	a := &Mongo{url: url, database: database, pool: pool, initialSession: session, ts: &ts}
	for _, i := range a.GetGraphs() {
		a.ts.Touch(i)
	}
	return a
}

// Close the connection
func (ma *Mongo) Close() {
	ma.pool.Close()
	ma.initialSession.Close()
	ma.initialSession = nil
}

func (ma *Mongo) getVertexCollection(session *mgo.Session, graph string) *mgo.Collection {
	return session.DB(ma.database).C(fmt.Sprintf("%s_vertices", graph))
}

func (ma *Mongo) getEdgeCollection(session *mgo.Session, graph string) *mgo.Collection {
	return session.DB(ma.database).C(fmt.Sprintf("%s_edges", graph))
}

// Graph is the tnterface to a single graph
type Graph struct {
	ar    *Mongo
	ts    *timestamp.Timestamp //BUG: This timestamp implementation doesn't work againt multiple mongo clients
	graph string
}

// AddGraph creates a new graph named `graph`
func (ma *Mongo) AddGraph(graph string) error {

	session := ma.pool.Get()

	graphs := session.DB(ma.database).C(fmt.Sprintf("graphs"))

	graphs.Insert(map[string]string{"_id": graph})

	//v := ma.db.C(fmt.Sprintf("%s_vertices", graph))
	e := ma.getEdgeCollection(session, graph)
	e.EnsureIndex(mgo.Index{Key: []string{"$hashed:from"}})
	e.EnsureIndex(mgo.Index{Key: []string{"$hashed:to"}})
	e.EnsureIndex(mgo.Index{Key: []string{"$hashed:label"}})

	v := ma.getVertexCollection(session, graph)
	v.EnsureIndex(mgo.Index{Key: []string{"$hashed:label"}})

	ma.ts.Touch(graph)
	ma.pool.Put(session)
	return nil
}

// DeleteGraph deletes `graph`
func (ma *Mongo) DeleteGraph(graph string) error {
	session := ma.pool.Get()
	g := session.DB(ma.database).C("graphs")
	v := ma.getVertexCollection(session, graph)
	e := ma.getEdgeCollection(session, graph)
	v.DropCollection()
	e.DropCollection()
	g.RemoveId(graph)
	ma.ts.Touch(graph)
	ma.pool.Put(session)
	return nil
}

// GetGraphs lists the graphs managed by this driver
func (ma *Mongo) GetGraphs() []string {
	session := ma.pool.Get()

	out := make([]string, 0, 100)
	g := session.DB(ma.database).C("graphs")

	iter := g.Find(nil).Iter()
	defer iter.Close()
	if err := iter.Err(); err != nil {
		log.Printf("Error: %s", err)
	}
	result := map[string]interface{}{}
	for iter.Next(&result) {
		out = append(out, result["_id"].(string))
	}
	if err := iter.Err(); err != nil {
		log.Printf("Error: %s", err)
	}
	log.Printf("Graphs: %s %s", ma.database, out)
	ma.pool.Put(session)
	return out
}

// Graph obtains the gdbi.DBI for a particular graph
func (ma *Mongo) Graph(graph string) gdbi.GraphInterface {
	return &Graph{
		ar:    ma,
		ts:    ma.ts,
		graph: graph,
	}
}

// GetEdge loads an edge given an id. It returns nil if not found
func (mg *Graph) GetEdge(id string, loadProp bool) *aql.Edge {
	session := mg.ar.pool.Get()
	d := map[string]interface{}{}
	q := mg.ar.getEdgeCollection(session, mg.graph).FindId(id)
	q.One(d)
	v := UnpackEdge(d)
	mg.ar.pool.Put(session)
	return v
}

//GetTimestamp gets the timestamp of last update
func (mg *Graph) GetTimestamp() string {
	return mg.ts.Get(mg.graph)
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (mg *Graph) GetVertex(key string, load bool) *aql.Vertex {
	session := mg.ar.pool.Get()
	d := map[string]interface{}{}
	vCol := mg.ar.getVertexCollection(session, mg.graph)
	q := vCol.Find(map[string]interface{}{"_id": key}).Limit(1)
	if !load {
		q = q.Select(map[string]interface{}{"_id": 1, "label": 1})
	}
	err := q.One(d)
	mg.ar.pool.Put(session)

	if err != nil {
		return nil
	}
	v := UnpackVertex(d)
	return v
}

// MaxRetries is the number of times driver will reconnect on connection failure
// TODO, move to per instance config, rather then global
var MaxRetries = 3

func isNetError(e error) bool {
	if e == io.EOF {
		return true
	}
	if b, ok := e.(*mgo.BulkError); ok {
		for _, c := range b.Cases() {
			if c.Err == io.EOF {
				return true
			}
			if strings.Contains(c.Err.Error(), "connection") {
				return true
			}
		}
	}
	return false
}

// AddVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (mg *Graph) AddVertex(vertexArray []*aql.Vertex) error {
	session := mg.ar.pool.Get()
	vCol := mg.ar.getVertexCollection(session, mg.graph)
	var err error
	for i := 0; i < MaxRetries; i++ {
		bulk := vCol.Bulk()
		for _, vertex := range vertexArray {
			bulk.Upsert(bson.M{"_id": vertex.Gid}, PackVertex(vertex))
		}
		_, err = bulk.Run()
		if err == nil || !isNetError(err) {
			mg.ts.Touch(mg.graph)
			mg.ar.pool.Put(session)
			return err
		}
		log.Printf("Refreshing Connection")
		session.Refresh()
	}
	mg.ar.pool.Put(session)
	return err
}

// AddEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (mg *Graph) AddEdge(edgeArray []*aql.Edge) error {
	session := mg.ar.pool.Get()
	eCol := mg.ar.getEdgeCollection(session, mg.graph)
	var err error
	for i := 0; i < MaxRetries; i++ {
		bulk := eCol.Bulk()
		for _, edge := range edgeArray {
			if edge.Gid != "" {
				bulk.Upsert(bson.M{"_id": edge.Gid}, PackEdge(edge))
			} else {
				edge.Gid = bson.NewObjectId().Hex()
				bulk.Insert(PackEdge(edge))
			}
		}
		_, err := bulk.Run()
		if err == nil || !isNetError(err) {
			mg.ts.Touch(mg.graph)
			mg.ar.pool.Put(session)
			return err
		}
		log.Printf("Refreshing Connection")
		session.Refresh()
	}
	mg.ar.pool.Put(session)
	return err
}

// DelVertex deletes vertex with id `key`
func (mg *Graph) DelVertex(key string) error {
	session := mg.ar.pool.Get()
	mg.ts.Touch(mg.graph)
	vCol := mg.ar.getVertexCollection(session, mg.graph)
	err := vCol.RemoveId(key)
	mg.ar.pool.Put(session)
	return err
}

// DelEdge deletes edge with id `key`
func (mg *Graph) DelEdge(key string) error {
	session := mg.ar.pool.Get()
	mg.ts.Touch(mg.graph)
	eCol := mg.ar.getEdgeCollection(session, mg.graph)
	err := eCol.RemoveId(key)
	mg.ar.pool.Put(session)
	return err
}

// GetVertexList produces a channel of all edges in the graph
func (mg *Graph) GetVertexList(ctx context.Context, load bool) <-chan *aql.Vertex {
	session := mg.ar.pool.Get()
	vCol := mg.ar.getVertexCollection(session, mg.graph)
	o := make(chan *aql.Vertex, 100)
	go func() {
		defer close(o)
		query := vCol.Find(nil)
		if !load {
			query = query.Select(bson.M{"_id": 1, "label": 1})
		}
		iter := query.Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				mg.ar.pool.Put(session)
				return
			default:
			}
			v := UnpackVertex(result)
			o <- v
		}
		mg.ar.pool.Put(session)
	}()
	return o
}

// GetEdgeList produces a channel of all edges in the graph
func (mg *Graph) GetEdgeList(ctx context.Context, loadProp bool) <-chan *aql.Edge {
	o := make(chan *aql.Edge, 100)
	session := mg.ar.pool.Get()
	eCol := mg.ar.getEdgeCollection(session, mg.graph)
	go func() {
		defer close(o)
		query := eCol.Find(nil)
		if !loadProp {
			query = query.Select(bson.M{"_id": 1, "to": 1, "from": 1, "label": 1})
		}
		iter := query.Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				mg.ar.pool.Put(session)
				return
			default:
			}
			if _, ok := result[fieldDst]; ok {
				log.Printf("Edge %s", result)
				e := UnpackEdge(result)
				o <- e
			} else if _, ok := result[fieldBundle]; ok {
				bundle := UnpackBundle(result)
				for k, v := range bundle.Bundle {
					e := &aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Data: v}
					o <- e
				}
			}
		}
		mg.ar.pool.Put(session)
	}()
	return o
}

// BatchSize controls size of batched mongo queries
//TODO: move this into driver config parameter
var BatchSize = 1000

// GetVertexChannel is passed a channel of vertex ids and it produces a channel
// of vertices
func (mg *Graph) GetVertexChannel(ids chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, BatchSize)
		for id := range ids {
			o = append(o, id)
			if len(o) >= BatchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, BatchSize)
			}
		}
		batches <- o
	}()

	out := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(out)
		session := mg.ar.pool.Get()
		vCol := mg.ar.getVertexCollection(session, mg.graph)
		for batch := range batches {
			//log.Printf("Getting Batch")
			idBatch := make([]string, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
			}
			query := bson.M{"_id": bson.M{"$in": idBatch}}
			//log.Printf("Query: %s", query)
			q := vCol.Find(query)
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
				chunk[v.Gid] = v
			}
			//if iter.Err() != nil {
			//	log.Printf("batch err: %s", iter.Err())
			//}

			for _, id := range batch {
				if x, ok := chunk[id.ID]; ok {
					id.Vertex = x
					out <- id
				}
			}
		}
		mg.ar.pool.Put(session)
	}()
	return out
}

//GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (mg *Graph) GetOutChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, BatchSize)
		for req := range reqChan {
			o = append(o, req)
			if len(o) >= BatchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, BatchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		session := mg.ar.pool.Get()

		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			query := []bson.M{{"$match": bson.M{"from": bson.M{"$in": idBatch}}}}
			if len(edgeLabels) > 0 {
				query = append(query, bson.M{"$match": bson.M{fieldLabel: bson.M{"$in": edgeLabels}}})
			}
			vertCol := fmt.Sprintf("%s_vertices", mg.graph)
			query = append(query, bson.M{"$lookup": bson.M{"from": vertCol, "localField": "to", "foreignField": "_id", "as": "dst"}})
			query = append(query, bson.M{"$unwind": "$dst"})
			if load {
				query = append(query, bson.M{"$project": bson.M{"from": true, fieldBundle: true, "dst._id": true, "dst.label": true, "dst.data": true}})
			} else {
				query = append(query, bson.M{"$project": bson.M{"from": true, fieldBundle: true, "dst._id": true, "dst.label": true}})
			}

			eCol := mg.ar.getEdgeCollection(session, mg.graph)
			iter := eCol.Pipe(query).Iter()
			defer iter.Close()
			result := map[string]interface{}{}
			for iter.Next(&result) {
				if val, ok := result[fieldBundle]; ok {
					vMap := val.(map[string]interface{})
					bkeys := make([]string, 0, len(vMap))
					for k := range vMap {
						bkeys = append(bkeys, k)
					}
					vCol := mg.ar.getVertexCollection(session, mg.graph)
					query := bson.M{"_id": bson.M{"$in": bkeys}}
					q := vCol.Find(query)
					vIter := q.Iter()
					r := batchMap[result["from"].(string)]
					vResult := map[string]interface{}{}
					for vIter.Next(&vResult) {
						v := UnpackVertex(vResult)
						for _, ri := range r {
							ri.Vertex = v
							o <- ri
						}
					}
					vIter.Close()
				} else if dst, ok := result["dst"].(map[string]interface{}); ok {
					v := UnpackVertex(dst)
					r := batchMap[result["from"].(string)]
					for _, ri := range r {
						ri.Vertex = v
						o <- ri
					}
				} else {
					log.Printf("Out Error: %s", result["dst"])
				}
			}
		}
		mg.ar.pool.Put(session)
	}()
	return o
}

//GetInChannel process requests of vertex ids and find the connected vertices on incoming edges
func (mg *Graph) GetInChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, BatchSize)
		for req := range reqChan {
			o = append(o, req)
			if len(o) >= BatchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, BatchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		session := mg.ar.pool.Get()

		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			query := []bson.M{{"$match": bson.M{"to": bson.M{"$in": idBatch}}}}
			if len(edgeLabels) > 0 {
				query = append(query, bson.M{"$match": bson.M{fieldLabel: bson.M{"$in": edgeLabels}}})
			}
			vertCol := fmt.Sprintf("%s_vertices", mg.graph)
			query = append(query, bson.M{"$lookup": bson.M{"from": vertCol, "localField": "from", "foreignField": "_id", "as": "src"}})
			query = append(query, bson.M{"$unwind": "$src"})
			if load {
				query = append(query, bson.M{"$project": bson.M{"to": true, fieldBundle: true, "src._id": true, "src.label": true, "src.data": true}})
			} else {
				query = append(query, bson.M{"$project": bson.M{"to": true, fieldBundle: true, "src._id": true, "src.label": true}})
			}

			eCol := mg.ar.getEdgeCollection(session, mg.graph)
			iter := eCol.Pipe(query).Iter()
			defer iter.Close()
			result := map[string]interface{}{}
			for iter.Next(&result) {
				src := result["src"].(map[string]interface{})
				v := UnpackVertex(src)
				r := batchMap[result["to"].(string)]
				for _, ri := range r {
					ri.Vertex = v
					o <- ri
				}
			}
			if err := iter.Err(); err != nil {
				log.Printf("Iteration Error %s", err)
			}
		}
		mg.ar.pool.Put(session)
	}()
	return o
}

//GetOutEdgeChannel process requests of vertex ids and find the connected outgoing edges
func (mg *Graph) GetOutEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, BatchSize)
		for req := range reqChan {
			o = append(o, req)
			if len(o) >= BatchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, BatchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		session := mg.ar.pool.Get()
		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			query := []bson.M{{"$match": bson.M{"from": bson.M{"$in": idBatch}}}}
			if len(edgeLabels) > 0 {
				query = append(query, bson.M{"$match": bson.M{fieldLabel: bson.M{"$in": edgeLabels}}})
			}
			eCol := mg.ar.getEdgeCollection(session, mg.graph)
			iter := eCol.Pipe(query).Iter()
			defer iter.Close()
			result := map[string]interface{}{}
			for iter.Next(&result) {
				if _, ok := result["bundle"]; ok {
					log.Printf("Bundle: %s", result)
					bundle := UnpackBundle(result)
					for k, v := range bundle.Bundle {
						e := aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Data: v}
						r := batchMap[result["from"].(string)]
						for _, ri := range r {
							ri.Edge = &e
							o <- ri
						}
					}
				} else {
					e := UnpackEdge(result)
					r := batchMap[result["from"].(string)]
					for _, ri := range r {
						ri.Edge = e
						o <- ri
					}
				}
			}
		}
		mg.ar.pool.Put(session)
	}()
	return o
}

//GetInEdgeChannel process requests of vertex ids and find the connected incoming edges
func (mg *Graph) GetInEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, BatchSize)
		for req := range reqChan {
			o = append(o, req)
			if len(o) >= BatchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, BatchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		session := mg.ar.pool.Get()
		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			query := []bson.M{{"$match": bson.M{"to": bson.M{"$in": idBatch}}}}
			if len(edgeLabels) > 0 {
				query = append(query, bson.M{"$match": bson.M{fieldLabel: bson.M{"$in": edgeLabels}}})
			}
			eCol := mg.ar.getEdgeCollection(session, mg.graph)
			iter := eCol.Pipe(query).Iter()
			defer iter.Close()
			result := map[string]interface{}{}
			for iter.Next(&result) {
				e := UnpackEdge(result)
				r := batchMap[result["to"].(string)]
				for _, ri := range r {
					ri.Edge = e
					o <- ri
				}
			}
		}
		mg.ar.pool.Put(session)
	}()
	return o
}

/*
// GetOutList given vertex/edge `key` find vertices on outgoing edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *Graph) GetOutList(ctx context.Context, key string, load bool, edgeLabels []string) <-chan *aql.Vertex {
	o := make(chan *aql.Vertex, 100)
	vertexChan := make(chan string, 100)
	go func() {
		defer close(vertexChan)
		session := mg.ar.pool.Get()

		eCol := mg.ar.getEdgeCollection(session, mg.graph)

		selection := map[string]interface{}{
			fieldSrc: key,
		}
		if len(edgeLabels) > 0 {
			selection[fieldLabel] = bson.M{"$in": edgeLabels}
		}
		iter := eCol.Find(selection).Iter()
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
		mg.ar.pool.Put(session)
	}()

	go func() {
		defer close(o)
		vCol := mg.ar.getVertexCollection(mg.graph)
		for dst := range vertexChan {
			q := vCol.FindId(dst)
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
*/

/*
// GetInList given vertex/edge `key` find vertices on incoming edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *Graph) GetInList(ctx context.Context, key string, load bool, edgeLabels []string) <-chan *aql.Vertex {
	//BUG: this should respond to both vertex and edge ids
	o := make(chan *aql.Vertex, 100)
	go func() {
		defer close(o)
		eCol := mg.ar.getEdgeCollection(mg.graph)
		vCol := mg.ar.getVertexCollection(mg.graph)
		selection := map[string]interface{}{
			fieldDst: key,
		}
		if len(edgeLabels) > 0 {
			selection[fieldLabel] = bson.M{"$in": edgeLabels}
		}
		iter := eCol.Find(selection).Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			q := vCol.FindId(result[fieldSrc])
			if !load {
				q = q.Select(map[string]interface{}{"_id": 1, "label": 1})
			}
			d := map[string]interface{}{}
			if err := q.One(d); err == nil {
				v := UnpackVertex(d)
				o <- v
			}
		}
	}()
	return o
}
*/

/*
// GetOutEdgeList given vertex `key` find all outgoing edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *Graph) GetOutEdgeList(ctx context.Context, key string, load bool, edgeLabels []string) <-chan *aql.Edge {
	o := make(chan *aql.Edge, 1000)
	go func() {
		defer close(o)
		eCol := mg.ar.getEdgeCollection(mg.graph)
		selection := map[string]interface{}{
			fieldSrc: key,
		}
		if len(edgeLabels) > 0 {
			selection[fieldLabel] = bson.M{"$in": edgeLabels}
		}
		iter := eCol.Find(selection).Iter()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			if _, ok := result[fieldDst]; ok {
				e := UnpackEdge(result)
				o <- e
			} else if _, ok := result[fieldBundle]; ok {
				bundle := UnpackBundle(result)
				for k, v := range bundle.Bundle {
					e := &aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Data: v}
					o <- e
				}
			}
		}
	}()
	return o
}
*/

// GetOutBundleChannel given vertex `key` find all outgoing bundles,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
// load is ignored
func (mg *Graph) GetOutBundleChannel(req chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 1000)
	go func() {
		defer close(o)
		session := mg.ar.pool.Get()
		eCol := mg.ar.getEdgeCollection(session, mg.graph)
		for r := range req {
			selection := map[string]interface{}{
				fieldSrc: r.ID,
			}
			if len(edgeLabels) > 0 {
				selection[fieldLabel] = bson.M{"$in": edgeLabels}
			}
			iter := eCol.Find(selection).Iter()
			result := map[string]interface{}{}
			for iter.Next(&result) {
				if _, ok := result[fieldBundle]; ok {
					bundle := UnpackBundle(result)
					r.Bundle = bundle
					o <- r
				}
			}
		}
		mg.ar.pool.Put(session)
	}()
	return o
}

/*
// GetInEdgeList given vertex `key` find all incoming edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (mg *Graph) GetInEdgeList(ctx context.Context, key string, load bool, edgeLabels []string) <-chan *aql.Edge {
	//TODO: use the load variable to filter data field from scan if possible
	o := make(chan *aql.Edge, 100)
	go func() {
		defer close(o)
		eCol := mg.ar.getEdgeCollection(mg.graph)

		selection := map[string]interface{}{
			fieldDst: key,
		}
		if len(edgeLabels) > 0 {
			selection[fieldLabel] = bson.M{"$in": edgeLabels}
		}
		iter := eCol.Find(selection).Iter()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			e := UnpackEdge(result)
			o <- e
		}
	}()
	return o
}
*/

// AddBundle adds a bundle to the graph
func (mg *Graph) AddBundle(bundle *aql.Bundle) error {
	session := mg.ar.pool.Get()
	eCol := mg.ar.getEdgeCollection(session, mg.graph)
	if bundle.Gid != "" {
		_, err := eCol.UpsertId(bundle.Gid, PackBundle(bundle))
		mg.ar.pool.Put(session)
		return err
	}
	err := eCol.Insert(PackBundle(bundle))
	mg.ts.Touch(mg.graph)
	mg.ar.pool.Put(session)
	return err
}

// GetBundle loads bundle of edges, given an id
// loadProp is ignored
func (mg *Graph) GetBundle(id string, loadProp bool) *aql.Bundle {
	d := map[string]interface{}{}
	session := mg.ar.pool.Get()
	eCol := mg.ar.getEdgeCollection(session, mg.graph)
	q := eCol.FindId(id)
	q.One(d)
	v := UnpackBundle(d)
	mg.ar.pool.Put(session)
	return v
}

// DelBundle removes a bundle of edges given an id
func (mg *Graph) DelBundle(id string) error {
	session := mg.ar.pool.Get()
	eCol := mg.ar.getEdgeCollection(session, mg.graph)
	err := eCol.RemoveId(id)
	mg.ts.Touch(mg.graph)
	mg.ar.pool.Put(session)
	return err
}
