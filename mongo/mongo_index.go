
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
	return mg.vertices.EnsureIndex(mgo.Index{Key: []string{"label", "data." + field}})
}

func (mg *Graph) AddEdgeIndex(label string, field string) error {
	return mg.edges.EnsureIndex(mgo.Index{Key: []string{"label", "data." + field}})
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
    pipe := mg.vertices.Pipe(ag)
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
    pipe := mg.edges.Pipe(ag)
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
  return mg.vertices.DropIndex("label", "data." + field)
}

func (mg *Graph) DeleteEdgeIndex(label string, field string) error {
  return mg.edges.DropIndex("label", "data." + field)
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
