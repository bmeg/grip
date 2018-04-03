package mongo

import (
	"context"
	"log"

	"github.com/bmeg/arachne/aql"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

//AddVertexIndex add index to vertices
func (mg *Graph) AddVertexIndex(label string, field string) error {
	log.Printf("Adding Index: %s %s", label, field)
	session := mg.ar.pool.Get()
	c := mg.ar.getVertexCollection(session, mg.graph)
	err := c.EnsureIndex(mgo.Index{Key: []string{"label", "data." + field}})
	if err != nil {
		log.Printf("Error: %s", err)
	}
	mg.ar.pool.Put(session)
	return err
}

//AddEdgeIndex add index to edges
/*
func (mg *Graph) AddEdgeIndex(label string, field string) error {
	session := mg.ar.pool.Get()
	c := mg.ar.getEdgeCollection(session, mg.graph)
	err := c.EnsureIndex(mgo.Index{Key: []string{"label", "data." + field}})
	mg.ar.pool.Put(session)
	return err
}
*/

// GetVertexIndexList lists indices
func (mg *Graph) GetVertexIndexList() chan aql.IndexID {
	log.Printf("IndexList")
	out := make(chan aql.IndexID)
	go func() {
		session := mg.ar.pool.Get()
		defer mg.ar.pool.Put(session)
		defer close(out)
		c := mg.ar.getVertexCollection(session, mg.graph)
		idxList, err := c.Indexes()
		if err != nil {
			log.Printf("Errors: %s", err)
			return
		}
		for _, idx := range idxList {
			log.Printf("Found index: %s", idx.Key)
			if len(idx.Key) > 1 && idx.Key[0] == "label" {
				out <- aql.IndexID{Graph: mg.graph, Field: idx.Key[1]}
			}
		}
	}()
	return out
}

//GetVertexTermCount get count of every term across vertices
func (mg *Graph) GetVertexTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount {
	out := make(chan aql.IndexTermCount, 100)
	go func() {
		defer close(out)
		session := mg.ar.pool.Get()
		defer mg.ar.pool.Put(session)
		ag := []bson.M{
			{"$match": bson.M{"label": label}},
			{"$group": bson.M{"_id": "$data." + field, "count": bson.M{"$sum": 1}}},
		}
		vcol := mg.ar.getVertexCollection(session, mg.graph)
		pipe := vcol.Pipe(ag)
		iter := pipe.Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			term := structpb.Value{Kind: &structpb.Value_StringValue{StringValue: result["_id"].(string)}}
			idxit := aql.IndexTermCount{Term: &term, Count: int32(result["count"].(int))}
			out <- idxit
		}
	}()
	return out
}

//GetEdgeTermCount get count of every term across edges
func (mg *Graph) GetEdgeTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount {
	out := make(chan aql.IndexTermCount, 100)
	go func() {
		defer close(out)
		session := mg.ar.pool.Get()
		defer mg.ar.pool.Put(session)
		ag := []bson.M{
			{"$match": bson.M{"label": label}},
			{"$group": bson.M{"_id": "$data." + field, "count": bson.M{"$sum": 1}}},
		}
		ecol := mg.ar.getEdgeCollection(session, mg.graph)
		pipe := ecol.Pipe(ag)
		iter := pipe.Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			term := structpb.Value{Kind: &structpb.Value_StringValue{StringValue: result["_id"].(string)}}
			idxit := aql.IndexTermCount{Term: &term, Count: int32(result["count"].(int))}
			out <- idxit
		}
	}()
	return out
}

//DeleteVertexIndex delete index from vertices
func (mg *Graph) DeleteVertexIndex(label string, field string) error {
	session := mg.ar.pool.Get()
	defer mg.ar.pool.Put(session)
	vcol := mg.ar.getVertexCollection(session, mg.graph)
	return vcol.DropIndex("label", "data."+field)
}

//DeleteEdgeIndex delete index from edges
func (mg *Graph) DeleteEdgeIndex(label string, field string) error {
	session := mg.ar.pool.Get()
	defer mg.ar.pool.Put(session)
	ecol := mg.ar.getEdgeCollection(session, mg.graph)
	return ecol.DropIndex("label", "data."+field)
}

// VertexLabelScan produces a channel of all edge ids where the edge label matches `label`
func (mg *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		session := mg.ar.pool.Get()
		defer mg.ar.pool.Put(session)
		selection := map[string]interface{}{
			"label": label,
		}
		vcol := mg.ar.getVertexCollection(session, mg.graph)
		iter := vcol.Find(selection).Select(map[string]interface{}{"_id": 1}).Iter()
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
		session := mg.ar.pool.Get()
		defer mg.ar.pool.Put(session)
		selection := map[string]interface{}{
			"label": label,
		}
		ecol := mg.ar.getEdgeCollection(session, mg.graph)
		iter := ecol.Find(selection).Select(map[string]interface{}{"_id": 1}).Iter()
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
