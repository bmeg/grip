package mongo

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/timestamp"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	log "github.com/sirupsen/logrus"
)

// Graph is the interface to a single graph
type Graph struct {
	ar *GraphDB
	//BUG: This timestamp implementation doesn't work againt multiple mongo clients
	ts        *timestamp.Timestamp
	graph     string
	batchSize int
}

// Compiler returns a query compiler that uses the graph
func (mg *Graph) Compiler() gdbi.Compiler {
	if !mg.ar.conf.UseAggregationPipeline {
		return core.NewCompiler(mg)
	}
	return NewCompiler(mg)
}

// GetTimestamp gets the timestamp of last update
func (mg *Graph) GetTimestamp() string {
	return mg.ts.Get(mg.graph)
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (mg *Graph) GetVertex(key string, load bool) *gripql.Vertex {
	session := mg.ar.session.Copy()
	defer session.Close()

	d := map[string]interface{}{}
	q := mg.ar.VertexCollection(session, mg.graph).FindId(key)
	if !load {
		q = q.Select(map[string]interface{}{"_id": 1, "label": 1})
	}
	err := q.One(d)
	if err != nil {
		return nil
	}

	v := UnpackVertex(d)
	return v
}

// GetEdge loads an edge given an id. It returns nil if not found
func (mg *Graph) GetEdge(id string, load bool) *gripql.Edge {
	session := mg.ar.session.Copy()
	defer session.Close()

	d := map[string]interface{}{}
	q := mg.ar.EdgeCollection(session, mg.graph).FindId(id)
	if !load {
		q = q.Select(map[string]interface{}{"_id": 1, "label": 1, "from": 1, "to": 1})
	}
	err := q.One(d)
	if err != nil {
		return nil
	}

	v := UnpackEdge(d)
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
func (mg *Graph) AddVertex(vertexArray []*gripql.Vertex) error {
	session := mg.ar.session.Copy()
	defer session.Close()

	vCol := mg.ar.VertexCollection(session, mg.graph)
	var err error
	for i := 0; i < MaxRetries; i++ {
		bulk := vCol.Bulk()
		for _, vertex := range vertexArray {
			bulk.Upsert(bson.M{"_id": vertex.Gid}, PackVertex(vertex))
		}
		_, err = bulk.Run()
		if err == nil || !isNetError(err) {
			mg.ts.Touch(mg.graph)
			return err
		}
		log.Info("Refreshing mongo connection")
		session.Refresh()
	}
	return err
}

// AddEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (mg *Graph) AddEdge(edgeArray []*gripql.Edge) error {
	session := mg.ar.session.Copy()
	defer session.Close()

	eCol := mg.ar.EdgeCollection(session, mg.graph)
	var err error
	for i := 0; i < MaxRetries; i++ {
		bulk := eCol.Bulk()
		for _, edge := range edgeArray {
			bulk.Upsert(bson.M{"_id": edge.Gid}, PackEdge(edge))
		}
		_, err = bulk.Run()
		if err == nil || !isNetError(err) {
			mg.ts.Touch(mg.graph)
			return err
		}
		log.Info("Refreshing mongo connection")
		session.Refresh()
	}
	return err
}

// deleteConnectedEdges deletes edges where `from` or `to` equal `key`
func (mg *Graph) deleteConnectedEdges(key string) error {
	session := mg.ar.session.Copy()
	defer session.Close()

	eCol := mg.ar.EdgeCollection(session, mg.graph)
	_, err := eCol.RemoveAll(bson.M{"$or": []bson.M{{"from": key}, {"to": key}}})
	if err != nil {
		return fmt.Errorf("failed to delete edge(s): %s", err)
	}
	mg.ts.Touch(mg.graph)
	return nil
}

// DelVertex deletes vertex with id `key`
func (mg *Graph) DelVertex(key string) error {
	session := mg.ar.session.Copy()
	defer session.Close()

	vCol := mg.ar.VertexCollection(session, mg.graph)
	err := vCol.RemoveId(key)
	if err != nil {
		return fmt.Errorf("failed to delete vertex %s: %s", key, err)
	}
	mg.ts.Touch(mg.graph)
	err = mg.deleteConnectedEdges(key)
	if err != nil {
		return err
	}
	return nil
}

// DelEdge deletes edge with id `key`
func (mg *Graph) DelEdge(key string) error {
	session := mg.ar.session.Copy()
	defer session.Close()

	eCol := mg.ar.EdgeCollection(session, mg.graph)
	err := eCol.RemoveId(key)
	if err != nil {
		return fmt.Errorf("failed to delete edge %s: %s", key, err)
	}
	mg.ts.Touch(mg.graph)
	return nil
}

// GetVertexList produces a channel of all vertices in the graph
func (mg *Graph) GetVertexList(ctx context.Context, load bool) <-chan *gripql.Vertex {
	o := make(chan *gripql.Vertex, 100)

	go func() {
		defer close(o)
		session := mg.ar.session.Copy()
		defer session.Close()
		vCol := mg.ar.VertexCollection(session, mg.graph)
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
				return
			default:
			}
			v := UnpackVertex(result)
			o <- v
		}
		if err := iter.Close(); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetVertexList")
		}
	}()

	return o
}

// GetEdgeList produces a channel of all edges in the graph
func (mg *Graph) GetEdgeList(ctx context.Context, loadProp bool) <-chan *gripql.Edge {
	o := make(chan *gripql.Edge, 100)

	go func() {
		defer close(o)
		session := mg.ar.session.Copy()
		defer session.Close()
		eCol := mg.ar.EdgeCollection(session, mg.graph)
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
				return
			default:
			}
			if _, ok := result["to"]; ok {
				e := UnpackEdge(result)
				o <- e
			}
		}
		if err := iter.Close(); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetEdgeList")
		}
	}()

	return o
}

// GetVertexChannel is passed a channel of vertex ids and it produces a channel
// of vertices
func (mg *Graph) GetVertexChannel(ids chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, mg.batchSize)
		for id := range ids {
			o = append(o, id)
			if len(o) >= mg.batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, mg.batchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		session := mg.ar.session.Copy()
		defer session.Close()
		vCol := mg.ar.VertexCollection(session, mg.graph)
		for batch := range batches {
			idBatch := make([]string, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
			}
			query := bson.M{"_id": bson.M{"$in": idBatch}}
			q := vCol.Find(query)
			if !load {
				q = q.Select(map[string]interface{}{"_id": 1, "label": 1})
			}
			iter := q.Iter()
			defer iter.Close()
			chunk := map[string]*gripql.Vertex{}
			result := map[string]interface{}{}
			for iter.Next(&result) {
				v := UnpackVertex(result)
				chunk[v.Gid] = v
			}
			if err := iter.Close(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetVertexChannel")
			}
			for _, id := range batch {
				if x, ok := chunk[id.ID]; ok {
					id.Vertex = x
					o <- id
				}
			}
		}
	}()
	return o
}

// GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (mg *Graph) GetOutChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, mg.batchSize)
		for req := range reqChan {
			o = append(o, req)
			if len(o) >= mg.batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, mg.batchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		session := mg.ar.session.Copy()
		defer session.Close()
		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			query := []bson.M{{"$match": bson.M{"from": bson.M{"$in": idBatch}}}}
			if len(edgeLabels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"label": bson.M{"$in": edgeLabels}}})
			}
			vertCol := fmt.Sprintf("%s_vertices", mg.graph)
			query = append(query, bson.M{"$lookup": bson.M{"from": vertCol, "localField": "to", "foreignField": "_id", "as": "dst"}})
			query = append(query, bson.M{"$unwind": "$dst"})
			if load {
				query = append(query, bson.M{"$project": bson.M{"from": true, "dst._id": true, "dst.label": true, "dst.data": true}})
			} else {
				query = append(query, bson.M{"$project": bson.M{"from": true, "dst._id": true, "dst.label": true}})
			}

			eCol := mg.ar.EdgeCollection(session, mg.graph)
			iter := eCol.Pipe(query).Iter()
			defer iter.Close()
			result := map[string]interface{}{}
			for iter.Next(&result) {
				if dst, ok := result["dst"].(map[string]interface{}); ok {
					v := UnpackVertex(dst)
					r := batchMap[result["from"].(string)]
					for _, ri := range r {
						ri.Vertex = v
						o <- ri
					}
				} else {
					log.WithFields(log.Fields{"result": result["dst"]}).Error("GetOutChannel: unable to cast result to map[string]interface{}")
				}
			}
			if err := iter.Close(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetOutChannel: iter error")
			}
		}
	}()
	return o
}

// GetInChannel process requests of vertex ids and find the connected vertices on incoming edges
func (mg *Graph) GetInChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, mg.batchSize)
		for req := range reqChan {
			o = append(o, req)
			if len(o) >= mg.batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, mg.batchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		session := mg.ar.session.Copy()
		defer session.Close()
		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			query := []bson.M{{"$match": bson.M{"to": bson.M{"$in": idBatch}}}}
			if len(edgeLabels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"label": bson.M{"$in": edgeLabels}}})
			}
			vertCol := fmt.Sprintf("%s_vertices", mg.graph)
			query = append(query, bson.M{"$lookup": bson.M{"from": vertCol, "localField": "from", "foreignField": "_id", "as": "src"}})
			query = append(query, bson.M{"$unwind": "$src"})
			if load {
				query = append(query, bson.M{"$project": bson.M{"to": true, "src._id": true, "src.label": true, "src.data": true}})
			} else {
				query = append(query, bson.M{"$project": bson.M{"to": true, "src._id": true, "src.label": true}})
			}

			eCol := mg.ar.EdgeCollection(session, mg.graph)
			iter := eCol.Pipe(query).Iter()
			defer iter.Close()
			result := map[string]interface{}{}
			for iter.Next(&result) {
				if src, ok := result["src"].(map[string]interface{}); ok {
					v := UnpackVertex(src)
					r := batchMap[result["to"].(string)]
					for _, ri := range r {
						ri.Vertex = v
						o <- ri
					}
				} else {
					log.WithFields(log.Fields{"result": result["src"]}).Error("GetInChannel: unable to cast result to map[string]interface{}")
				}
			}
			if err := iter.Close(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetInChannel: iter error")
			}
		}
	}()
	return o
}

// GetOutEChannel process requests of vertex ids and find the connected outgoing edges
func (mg *Graph) GetOutEChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, mg.batchSize)
		for req := range reqChan {
			o = append(o, req)
			if len(o) >= mg.batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, mg.batchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		session := mg.ar.session.Copy()
		defer session.Close()
		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			query := []bson.M{{"$match": bson.M{"from": bson.M{"$in": idBatch}}}}
			if len(edgeLabels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"label": bson.M{"$in": edgeLabels}}})
			}
			eCol := mg.ar.EdgeCollection(session, mg.graph)
			iter := eCol.Pipe(query).Iter()
			defer iter.Close()
			result := map[string]interface{}{}
			for iter.Next(&result) {
				e := UnpackEdge(result)
				r := batchMap[result["from"].(string)]
				for _, ri := range r {
					ri.Edge = e
					o <- ri
				}
			}
			if err := iter.Close(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetOutEChannel: iter error")
			}
		}
	}()

	return o
}

// GetInEChannel process requests of vertex ids and find the connected incoming edges
func (mg *Graph) GetInEChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, mg.batchSize)
		for req := range reqChan {
			o = append(o, req)
			if len(o) >= mg.batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, mg.batchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		session := mg.ar.session.Copy()
		defer session.Close()
		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			query := []bson.M{{"$match": bson.M{"to": bson.M{"$in": idBatch}}}}
			if len(edgeLabels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"label": bson.M{"$in": edgeLabels}}})
			}
			eCol := mg.ar.EdgeCollection(session, mg.graph)
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
			if err := iter.Close(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetInEChannel: iter error")
			}
		}
	}()

	return o
}
