package mongo

import (
	"context"
	"fmt"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// AddVertexIndex add index to vertices
func (mg *Graph) AddVertexIndex(field string) error {
	log.WithFields(log.Fields{"field": field}).Info("Adding vertex index")
	field = jsonpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	idx := mg.ar.VertexCollection(mg.graph).Indexes()

	_, err := idx.CreateOne(
		context.Background(),
		mongo.IndexModel{
			Keys:    bson.M{field: 1},
			Options: options.Index().SetUnique(false).SetSparse(true).SetBackground(true),
		})
	if err != nil {
		return fmt.Errorf("failed create index %s %s", field, err)
	}
	return nil
}

// DeleteVertexIndex delete index from vertices
func (mg *Graph) DeleteVertexIndex(field string) error {
	log.WithFields(log.Fields{"field": field}).Info("Deleting vertex index")
	field = jsonpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	idx := mg.ar.VertexCollection(mg.graph).Indexes()
	cursor, err := idx.List(context.TODO())
	var results []bson.M
	if err = cursor.All(context.TODO(), &results); err != nil {
		return err
	}
	for _, rec := range results {
		recKeys := rec["key"].(bson.M)
		if _, ok := recKeys[field]; ok {
			if _, err := idx.DropOne(context.TODO(), rec["name"].(string)); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetVertexIndexList lists indices
func (mg *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	log.Debug("Running GetVertexIndexList")
	out := make(chan *gripql.IndexID)

	go func() {
		defer close(out)
		c := mg.ar.VertexCollection(mg.graph)

		// list indexed fields
		idx := c.Indexes()
		cursor, err := idx.List(context.TODO())
		var idxList []bson.M
		if err = cursor.All(context.TODO(), &idxList); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetVertexIndexList: finding indexed fields")
		}
		for _, rec := range idxList {
			recKeys := rec["key"].(bson.M)
			key := ""
			for k := range recKeys {
				if k != "label" {
					key = k
				}
			}
			if len(key) > 0 {
				f := strings.TrimPrefix(key, "data.")
				out <- &gripql.IndexID{Graph: mg.graph, Field: f}
			}
		}
	}()

	return out
}

// VertexLabelScan produces a channel of all vertex ids where the vertex label matches `label`
func (mg *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	log.WithFields(log.Fields{"label": label}).Debug("Running VertexLabelScan")
	out := make(chan string, 100)
	go func() {
		defer close(out)
		selection := map[string]interface{}{
			"label": label,
		}
		vcol := mg.ar.VertexCollection(mg.graph)
		opts := options.Find()
		opts.SetProjection(map[string]interface{}{"_id": 1, "label": 1})

		cursor, err := vcol.Find(context.TODO(), selection, opts)
		if err == nil {
			defer cursor.Close(context.TODO())
			result := map[string]interface{}{}
			for cursor.Next(context.TODO()) {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if nil == cursor.Decode(&result) {
					out <- result["_id"].(string)
				}
			}
			if err := cursor.Close(context.TODO()); err != nil {
				log.Errorln("VertexLabelScan error:", err)
			}
		}
	}()
	return out
}

func (mg *Graph) VertexIndexScan(ctx context.Context, query *gripql.SearchQuery) <-chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)

		selection := map[string]interface{}{}

		reg := fmt.Sprintf("^%s", query.Term)
		if len(query.Fields) == 1 {
			field := convertPath(query.Fields[0])
			selection[field] = bson.M{"$regex": reg}
		} else {
			a := []interface{}{}
			for _, i := range query.Fields {
				field := convertPath(i)
				a = append(a, bson.M{field: bson.M{"$regex": reg}})
			}
			selection["$or"] = a
		}
		vcol := mg.ar.VertexCollection(mg.graph)
		opts := options.Find()
		opts.SetProjection(map[string]interface{}{"_id": 1})
		cursor, err := vcol.Find(ctx, selection, opts)
		if err == nil {
			defer cursor.Close(context.TODO())
			result := map[string]interface{}{}
			for cursor.Next(context.TODO()) {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if nil == cursor.Decode(&result) {
					out <- result["_id"].(string)
				}
			}
		}
	}()
	return out
}
