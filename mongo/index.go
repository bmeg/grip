package mongo

import (
	"context"
	"fmt"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/travelerpath"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// AddVertexIndex add index to vertices
func (mg *Graph) AddVertexIndex(label string, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Adding vertex index")
	field = travelerpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	idx := mg.ar.VertexCollection(mg.graph).Indexes()

	_, err := idx.CreateOne(
		context.Background(),
		mongo.IndexModel{
			Keys:    bson.D{{"label", 1}, {field, 1}},
			Options: options.Index().SetUnique(false).SetSparse(true).SetBackground(true),
		})
	if err != nil {
		return fmt.Errorf("failed create index %s %s %s", label, field, err)
	}
	return nil
}

// DeleteVertexIndex delete index from vertices
func (mg *Graph) DeleteVertexIndex(label string, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Deleting vertex index")
	field = travelerpath.GetJSONPath(field)
	field = strings.TrimPrefix(field, "$.")

	idx := mg.ar.VertexCollection(mg.graph).Indexes()
	cursor, err := idx.List(context.TODO())
	var results []bson.M
	if err = cursor.All(context.TODO(), &results); err != nil {
		return err
	}
	for _, rec := range results {
		recKeys := rec["key"].(bson.M)
		if _, ok := recKeys["label"]; ok {
			if _, ok := recKeys[field]; ok {
				if _, err := idx.DropOne(context.TODO(), rec["name"].(string)); err != nil {
					return err
				}
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

		labels, err := mg.ListVertexLabels()
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetVertexIndexList: finding distinct labels")
		}

		// list indexed fields
		idx := c.Indexes()
		cursor, err := idx.List(context.TODO())
		var idxList []bson.M
		if err = cursor.All(context.TODO(), &idxList); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetVertexIndexList: finding indexed fields")
		}
		for _, rec := range idxList {
			recKeys := rec["key"].(bson.M)
			if len(recKeys) > 1 {
				if _, ok := recKeys["label"]; ok {
					key := ""
					for k := range recKeys {
						if k != "label" {
							key = k
						}
					}
					if len(key) > 0 {
						f := strings.TrimPrefix(key, "data.")
						for _, l := range labels {
							out <- &gripql.IndexID{Graph: mg.graph, Label: l, Field: f}
						}
					}
				}
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
