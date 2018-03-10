
package mongo

import (
  "log"
  "github.com/bmeg/arachne/aql"
  "context"
  "gopkg.in/mgo.v2/bson"
  "gopkg.in/mgo.v2"
  structpb "github.com/golang/protobuf/ptypes/struct"
)

func (mg *Graph) AddVertexIndex(label string, field string) error {
  log.Printf("Adding Index: %s", field)
  c := mg.ar.getVertexCollection(mg.graph)
	return c.EnsureIndex(mgo.Index{Key: []string{"label", "data." + field}})
}

func (mg *Graph) AddEdgeIndex(label string, field string) error {
	c := mg.ar.getEdgeCollection(mg.graph)
  return c.EnsureIndex(mgo.Index{Key: []string{"label", "data." + field}})
}

func (mg *Graph) GetVertexTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount {
  log.Printf("Listing Index: %s %s", label, field)
  out := make(chan aql.IndexTermCount, 100)
  go func() {
    defer close(out)
    ag := []bson.M{
      {"$match": bson.M{"label": label}},
      {"$group": bson.M{"_id": "$data." + field, "count": bson.M{"$sum":1}}},
    }
    vcol := mg.ar.getVertexCollection(mg.graph)
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
      idxit := aql.IndexTermCount{Term:&term, Count:int32(result["count"].(int))}
      out <- idxit
    }
  }()
  return out
}

func (mg *Graph) GetEdgeTermCount(ctx context.Context, label string, field string) chan aql.IndexTermCount {
  out := make(chan aql.IndexTermCount, 100)
  go func() {
    defer close(out)
    ag := []bson.M{
      {"$match": bson.M{"label": label}},
      {"$group": bson.M{"_id": "$data." + field, "count": bson.M{"$sum":1}}},
    }
    ecol := mg.ar.getEdgeCollection(mg.graph)
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
      idxit := aql.IndexTermCount{Term:&term, Count:int32(result["count"].(int))}
      out <- idxit
    }
  }()
  return out
}

func (mg *Graph) DeleteVertexIndex(label string, field string) error {
  vcol := mg.ar.getVertexCollection(mg.graph)
  return vcol.DropIndex("label", "data." + field)
}

func (mg *Graph) DeleteEdgeIndex(label string, field string) error {
  ecol := mg.ar.getEdgeCollection(mg.graph)
  return ecol.DropIndex("label", "data." + field)
}


// VertexLabelScan produces a channel of all edge ids where the edge label matches `label`
func (mg *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		selection := map[string]interface{}{
			"label": label,
		}
    vcol := mg.ar.getVertexCollection(mg.graph)
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
		selection := map[string]interface{}{
			"label": label,
		}
    ecol := mg.ar.getEdgeCollection(mg.graph)
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
