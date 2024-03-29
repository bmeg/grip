package mongo

import (
	"context"
	"fmt"

	//"io"
	//"strings"

	"time"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
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
	if mg.ar.conf.UseCorePipeline {
		return core.NewCompiler(mg, core.IndexStartOptimize) //TODO: probably a better optimizer for vertex label search
	}
	return NewCompiler(mg)
}

// GetTimestamp gets the timestamp of last update
func (mg *Graph) GetTimestamp() string {
	return mg.ts.Get(mg.graph)
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (mg *Graph) GetVertex(id string, load bool) *gdbi.Vertex {
	opts := options.FindOne()
	if !load {
		opts.SetProjection(map[string]interface{}{"_id": 1, "label": 1})
	}
	result := mg.ar.VertexCollection(mg.graph).FindOne(context.Background(), bson.M{"_id": id}, opts)
	if result.Err() != nil {
		return nil
	}
	d := map[string]interface{}{}
	if nil == result.Decode(d) {
		v := UnpackVertex(d)
		return v
	}
	return nil
}

// GetEdge loads an edge given an id. It returns nil if not found
func (mg *Graph) GetEdge(id string, load bool) *gdbi.Edge {
	opts := options.FindOne()
	if !load {
		opts.SetProjection(map[string]interface{}{"_id": 1, "label": 1, "from": 1, "to": 1})
	}
	result := mg.ar.EdgeCollection(mg.graph).FindOne(context.TODO(), bson.M{"_id": id}, opts)
	if result.Err() != nil {
		return nil
	}
	d := map[string]interface{}{}
	if nil == result.Decode(d) {
		v := UnpackEdge(d)
		return v
	}
	return nil
}

// AddVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (mg *Graph) AddVertex(vertices []*gdbi.Vertex) error {
	vCol := mg.ar.VertexCollection(mg.graph)
	var err error
	docBatch := make([]mongo.WriteModel, 0, len(vertices))
	for _, v := range vertices {
		i := mongo.NewReplaceOneModel().SetUpsert(true).SetFilter(bson.M{"_id": v.ID})
		ent := PackVertex(v)
		i.SetReplacement(ent)
		docBatch = append(docBatch, i)
	}
	if len(docBatch) > 0 {
		_, err = vCol.BulkWrite(context.Background(), docBatch)
		if err != nil {
			log.Errorf("AddVertex error: (%s) %s", docBatch, err)
		}
	}
	return err
}

// AddEdge adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (mg *Graph) AddEdge(edges []*gdbi.Edge) error {
	eCol := mg.ar.EdgeCollection(mg.graph)
	var err error
	docBatch := make([]mongo.WriteModel, 0, len(edges))
	for _, edge := range edges {
		i := mongo.NewReplaceOneModel().SetUpsert(true).SetFilter(bson.M{"_id": edge.ID})
		ent := PackEdge(edge)
		i.SetReplacement(ent)
		docBatch = append(docBatch, i)
	}
	if len(docBatch) > 0 {
		_, err = eCol.BulkWrite(context.Background(), docBatch)
	}
	return err
}

func (mg *Graph) BulkAdd(stream <-chan *gdbi.GraphElement) error {
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
func (mg *Graph) GetVertexList(ctx context.Context, load bool) <-chan *gdbi.Vertex {
	o := make(chan *gdbi.Vertex, 100)

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
			if err := query.Decode(&result); err == nil {
				v := UnpackVertex(result)
				o <- v
			} else {
				log.Errorf("Error decoding edge %#v", result)
			}
		}
	}()

	return o
}

// GetEdgeList produces a channel of all edges in the graph
func (mg *Graph) GetEdgeList(ctx context.Context, loadProp bool) <-chan *gdbi.Edge {
	o := make(chan *gdbi.Edge, 100)

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
			if err := query.Decode(&result); err == nil {
				if _, ok := result["to"]; ok {
					e := UnpackEdge(result)
					o <- e
				}
			} else {
				log.Errorf("Error decoding edge %#v", result)
			}
		}
	}()

	return o
}

// GetVertexChannel is passed a channel of vertex ids and it produces a channel
// of vertices
func (mg *Graph) GetVertexChannel(ctx context.Context, ids chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	batches := gdbi.LookupBatcher(ids, mg.batchSize, time.Microsecond)

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		vCol := mg.ar.VertexCollection(mg.graph)
		for batch := range batches {
			idBatch := make([]string, 0, len(batch))
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
				} else {
					idBatch = append(idBatch, batch[i].ID)
				}
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
			chunk := map[string]*gdbi.Vertex{}
			result := map[string]interface{}{}
			for cursor.Next(context.TODO()) {
				if err := cursor.Decode(&result); err == nil {
					v := UnpackVertex(result)
					chunk[v.ID] = v
				} else {
					log.WithFields(log.Fields{"error": err}).Error("Decode error")
				}
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
			for i := range signals {
				o <- signals[i]
			}
		}
	}()
	return o
}

// GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (mg *Graph) GetOutChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := gdbi.LookupBatcher(reqChan, mg.batchSize, time.Microsecond)

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for batch := range batches {
			idBatch := make([]string, 0, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			batchMapReturnCount := make(map[string]int, len(batch))
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
				} else {
					idBatch = append(idBatch, batch[i].ID)
					batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
					batchMapReturnCount[batch[i].ID] = 0
				}
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
					if err := cursor.Decode(&result); err == nil {
						if dst, ok := result["dst"].(map[string]interface{}); ok {
							v := UnpackVertex(dst)
							fromID := result["from"].(string)
							r := batchMap[fromID]
							batchMapReturnCount[fromID]++
							for _, ri := range r {
								ri.Vertex = v
								o <- ri
							}
						} else {
							log.WithFields(log.Fields{"result": result["dst"]}).Error("GetOutChannel: unable to cast result to map[string]interface{}")
						}
					} else {
						log.WithFields(log.Fields{"result": result, "error": err}).Error("GetOutChannel: decode error")
					}
				}
				if err := cursor.Close(context.TODO()); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetOutChannel: iter error")
				}
				if emitNull {
					for id, count := range batchMapReturnCount {
						if count == 0 {
							r := batchMap[id]
							for _, ri := range r {
								ri.Vertex = nil
								o <- ri
							}
						}
					}
				}
			}
			for i := range signals {
				o <- signals[i]
			}
		}
	}()
	return o
}

// GetInChannel process requests of vertex ids and find the connected vertices on incoming edges
func (mg *Graph) GetInChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := gdbi.LookupBatcher(reqChan, mg.batchSize, time.Microsecond)

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for batch := range batches {
			idBatch := make([]string, 0, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			batchMapReturnCount := make(map[string]int, len(batch))
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
				} else {
					idBatch = append(idBatch, batch[i].ID)
					batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
					batchMapReturnCount[batch[i].ID] = 0
				}
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
					if err := cursor.Decode(&result); err == nil {
						if src, ok := result["src"].(map[string]interface{}); ok {
							v := UnpackVertex(src)
							toID := result["to"].(string)
							r := batchMap[toID]
							batchMapReturnCount[toID]++
							for _, ri := range r {
								ri.Vertex = v
								o <- ri
							}
						} else {
							log.WithFields(log.Fields{"result": result["src"]}).Error("GetInChannel: unable to cast result to map[string]interface{}")
						}
					} else {
						log.WithFields(log.Fields{"error": err}).Error("Decode")
					}
				}
				if err := cursor.Close(context.TODO()); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetInChannel: iter error")
				}
				if emitNull {
					for id, count := range batchMapReturnCount {
						if count == 0 {
							r := batchMap[id]
							for _, ri := range r {
								ri.Vertex = nil
								o <- ri
							}
						}
					}
				}
			}
			for i := range signals {
				o <- signals[i]
			}
		}
	}()
	return o
}

// GetOutEdgeChannel process requests of vertex ids and find the connected outgoing edges
func (mg *Graph) GetOutEdgeChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := gdbi.LookupBatcher(reqChan, mg.batchSize, time.Microsecond)

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for batch := range batches {
			idBatch := make([]string, 0, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			batchMapReturnCount := make(map[string]int, len(batch))
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
				} else {
					idBatch = append(idBatch, batch[i].ID)
					batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
					batchMapReturnCount[batch[i].ID] = 0
				}
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
					if err := cursor.Decode(&result); err == nil {
						e := UnpackEdge(result)
						fromID := result["from"].(string)
						r := batchMap[fromID]
						batchMapReturnCount[fromID]++
						for _, ri := range r {
							ri.Edge = e
							o <- ri
						}
					} else {
						log.WithFields(log.Fields{"error": err}).Error("Decode")
					}
				}
				if err := cursor.Close(context.TODO()); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetOutEdgeChannel: iter error")
				}
				if emitNull {
					for id, count := range batchMapReturnCount {
						if count == 0 {
							r := batchMap[id]
							for _, ri := range r {
								ri.Edge = nil
								o <- ri
							}
						}
					}
				}
			}
			for i := range signals {
				o <- signals[i]
			}
		}
	}()

	return o
}

// GetInEdgeChannel process requests of vertex ids and find the connected incoming edges
func (mg *Graph) GetInEdgeChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := gdbi.LookupBatcher(reqChan, mg.batchSize, time.Microsecond)

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for batch := range batches {
			idBatch := make([]string, 0, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			batchMapReturnCount := make(map[string]int, len(batch))
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
				} else {
					idBatch = append(idBatch, batch[i].ID)
					batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
					batchMapReturnCount[batch[i].ID] = 0
				}
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
					if err := cursor.Decode(&result); err == nil {
						e := UnpackEdge(result)
						toID := result["to"].(string)
						r := batchMap[toID]
						batchMapReturnCount[toID]++
						for _, ri := range r {
							ri.Edge = e
							o <- ri
						}
					} else {
						log.WithFields(log.Fields{"error": err}).Error("Decode")
					}
				}
				if err := cursor.Close(context.TODO()); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetInEdgeChannel: iter error")
				}
				if emitNull {
					for id, count := range batchMapReturnCount {
						if count == 0 {
							r := batchMap[id]
							for _, ri := range r {
								ri.Edge = nil
								o <- ri
							}
						}
					}
				}
			}
			for i := range signals {
				o <- signals[i]
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
