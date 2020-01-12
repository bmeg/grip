package mongo

import (
	"context"
	"fmt"

	//"io"
	//"strings"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/timestamp"
	"github.com/bmeg/grip/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
func (mg *Graph) GetVertex(id string, load bool) *gripql.Vertex {
	opts := options.FindOne()
	if !load {
		opts.SetProjection(map[string]interface{}{"_id": 1, "label": 1})
	}
	result := mg.ar.VertexCollection(mg.graph).FindOne(context.Background(), bson.M{"_id": id}, opts)
	if result.Err() != nil {
		return nil
	}
	d := map[string]interface{}{}
	result.Decode(d)
	v := UnpackVertex(d)
	return v
}

// GetEdge loads an edge given an id. It returns nil if not found
func (mg *Graph) GetEdge(id string, load bool) *gripql.Edge {
	opts := options.FindOne()
	if !load {
		opts.SetProjection(map[string]interface{}{"_id": 1, "label": 1, "from": 1, "to": 1})
	}
	result := mg.ar.EdgeCollection(mg.graph).FindOne(context.TODO(), bson.M{"_id": id}, opts)
	if result.Err() != nil {
		return nil
	}
	d := map[string]interface{}{}
	result.Decode(d)
	v := UnpackEdge(d)
	return v
}

// AddVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (mg *Graph) AddVertex(vertices []*gripql.Vertex) error {
	vCol := mg.ar.VertexCollection(mg.graph)
	var err error
	docBatch := make([]mongo.WriteModel, 0, len(vertices))
	for _, v := range vertices {
		i := mongo.NewReplaceOneModel().SetUpsert(true).SetFilter(bson.M{"_id": v.Gid})
		ent := PackVertex(v)
		i.SetReplacement(ent)
		docBatch = append(docBatch, i)
	}
	_, err = vCol.BulkWrite(context.Background(), docBatch)
	if err != nil {
		log.Errorf("AddVertex error: (%s) %s", docBatch, err)
	}
	return err
}

// AddEdge adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (mg *Graph) AddEdge(edges []*gripql.Edge) error {
	eCol := mg.ar.EdgeCollection(mg.graph)
	var err error
	docBatch := make([]mongo.WriteModel, 0, len(edges))
	for _, edge := range edges {
		i := mongo.NewReplaceOneModel().SetUpsert(true).SetFilter(bson.M{"_id": edge.Gid})
		ent := PackEdge(edge)
		i.SetReplacement(ent)
		docBatch = append(docBatch, i)
	}
	_, err = eCol.BulkWrite(context.Background(), docBatch)
	return err
}

func (mg *Graph) BulkAdd(stream <-chan *gripql.GraphElement) error {
	return util.StreamBatch(stream, 50, mg.graph, mg.AddVertex, mg.AddEdge)
}

// deleteConnectedEdges deletes edges where `from` or `to` equal `key`
func (mg *Graph) deleteConnectedEdges(key string) error {
	eCol := mg.ar.EdgeCollection(mg.graph)
	_, err := eCol.DeleteMany(context.TODO(), bson.M{"$or": []bson.M{{"from": key}, {"to": key}}})
	if err != nil {
		return fmt.Errorf("failed to delete edge(s): %s", err)
	}
	mg.ts.Touch(mg.graph)
	return nil
}

// DelVertex deletes vertex with id `key`
func (mg *Graph) DelVertex(key string) error {
	vCol := mg.ar.VertexCollection(mg.graph)
	_, err := vCol.DeleteOne(context.TODO(), bson.M{"_id": key})
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
	eCol := mg.ar.EdgeCollection(mg.graph)
	_, err := eCol.DeleteOne(context.TODO(), bson.M{"_id": key})
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
		vCol := mg.ar.VertexCollection(mg.graph)
		opts := options.Find()
		if !load {
			opts.SetProjection(bson.M{"_id": 1, "label": 1})
		}
		query, err := vCol.Find(ctx, bson.M{}, opts)
		if err != nil {
			return
		}
		defer query.Close(ctx)
		result := map[string]interface{}{}
		for query.Next(ctx) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			query.Decode(&result)
			v := UnpackVertex(result)
			o <- v
		}
	}()

	return o
}

// GetEdgeList produces a channel of all edges in the graph
func (mg *Graph) GetEdgeList(ctx context.Context, loadProp bool) <-chan *gripql.Edge {
	o := make(chan *gripql.Edge, 100)

	go func() {
		defer close(o)
		eCol := mg.ar.EdgeCollection(mg.graph)
		opts := options.Find()
		if !loadProp {
			opts.SetProjection(bson.M{"_id": 1, "to": 1, "from": 1, "label": 1})
		}
		query, err := eCol.Find(ctx, bson.M{}, opts)
		if err != nil {
			return
		}
		defer query.Close(ctx)
		result := map[string]interface{}{}
		for query.Next(ctx) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			query.Decode(&result)
			if _, ok := result["to"]; ok {
				e := UnpackEdge(result)
				o <- e
			}
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
		vCol := mg.ar.VertexCollection(mg.graph)
		for batch := range batches {
			idBatch := make([]string, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
			}
			query := bson.M{"_id": bson.M{"$in": idBatch}}
			opts := options.Find()
			if !load {
				opts.SetProjection(bson.M{"_id": 1, "label": 1})
			}
			cursor, err := vCol.Find(context.TODO(), query, opts)
			if err != nil {
				return
			}
			chunk := map[string]*gripql.Vertex{}
			result := map[string]interface{}{}
			for cursor.Next(context.TODO()) {
				cursor.Decode(&result)
				v := UnpackVertex(result)
				chunk[v.Gid] = v
			}
			if err := cursor.Close(context.TODO()); err != nil {
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

			eCol := mg.ar.EdgeCollection(mg.graph)
			cursor, err := eCol.Aggregate(context.TODO(), query)
			if err == nil {
				result := map[string]interface{}{}
				for cursor.Next(context.TODO()) {
					cursor.Decode(&result)
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
				if err := cursor.Close(context.TODO()); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetOutChannel: iter error")
				}
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

			eCol := mg.ar.EdgeCollection(mg.graph)
			cursor, err := eCol.Aggregate(context.TODO(), query)
			if err == nil {
				result := map[string]interface{}{}
				for cursor.Next(context.TODO()) {
					cursor.Decode(&result)
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
				if err := cursor.Close(context.TODO()); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetInChannel: iter error")
				}
			}
		}
	}()
	return o
}

// GetOutEdgeChannel process requests of vertex ids and find the connected outgoing edges
func (mg *Graph) GetOutEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
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
			eCol := mg.ar.EdgeCollection(mg.graph)
			cursor, err := eCol.Aggregate(context.TODO(), query)
			if err == nil {
				result := map[string]interface{}{}
				for cursor.Next(context.TODO()) {
					cursor.Decode(&result)
					e := UnpackEdge(result)
					r := batchMap[result["from"].(string)]
					for _, ri := range r {
						ri.Edge = e
						o <- ri
					}
				}
				if err := cursor.Close(context.TODO()); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetOutEdgeChannel: iter error")
				}
			}
		}
	}()

	return o
}

// GetInEdgeChannel process requests of vertex ids and find the connected incoming edges
func (mg *Graph) GetInEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
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
			eCol := mg.ar.EdgeCollection(mg.graph)
			cursor, err := eCol.Aggregate(context.TODO(), query)
			if err == nil {
				result := map[string]interface{}{}
				for cursor.Next(context.TODO()) {
					cursor.Decode(&result)
					e := UnpackEdge(result)
					r := batchMap[result["to"].(string)]
					for _, ri := range r {
						ri.Edge = e
						o <- ri
					}
				}
				if err := cursor.Close(context.TODO()); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetInEdgeChannel: iter error")
				}
			}
		}
	}()

	return o
}

// ListVertexLabels returns a list of vertex types in the graph
func (mg *Graph) ListVertexLabels() ([]string, error) {
	v := mg.ar.VertexCollection(mg.graph)
	out, err := v.Distinct(context.TODO(), "label", bson.M{})
	if err != nil {
		return nil, err
	}
	labels := make([]string, len(out))
	for i := range out {
		labels[i] = out[i].(string)
	}
	return labels, nil
}

// ListEdgeLabels returns a list of edge types in the graph
func (mg *Graph) ListEdgeLabels() ([]string, error) {
	e := mg.ar.EdgeCollection(mg.graph)
	out, err := e.Distinct(context.TODO(), "label", bson.M{})
	if err != nil {
		return nil, err
	}
	labels := make([]string, len(out))
	for i := range out {
		labels[i] = out[i].(string)
	}
	return labels, nil
}
