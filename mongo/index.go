package mongo

import (
	"context"
	"fmt"
	"strings"

	"github.com/bmeg/grip/gdbi/tpath"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// AddVertexIndex adds an index to vertices for a specific label and field
func (mg *Graph) AddVertexIndex(label string, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Adding vertex index")
	field = tpath.NormalizePath(field)
	field = tpath.ToLocalPath(field)
	field = strings.TrimPrefix(field, "$.")

	idx := mg.ar.VertexCollection(mg.graph).Indexes()

	// Create a compound index on _label and the specified field, filtered by the specific label
	_, err := idx.CreateOne(
		context.Background(),
		mongo.IndexModel{
			Keys: bson.D{
				{Key: FIELD_LABEL, Value: 1},
				{Key: field, Value: 1},
			},
			Options: options.Index().
				SetUnique(false).
				SetBackground(true).
				SetPartialFilterExpression(bson.M{FIELD_LABEL: label}),
		})
	if err != nil {
		return fmt.Errorf("failed to create index for label %s on field %s: %s", label, field, err)
	}
	return nil
}

// DeleteVertexIndex deletes an index from vertices for a specific label and field
func (mg *Graph) DeleteVertexIndex(label string, field string) error {
	log.WithFields(log.Fields{"label": label, "field": field}).Info("Deleting vertex index")
	field = tpath.NormalizePath(field)
	field = tpath.ToLocalPath(field)
	field = strings.TrimPrefix(field, "$.")

	idx := mg.ar.VertexCollection(mg.graph).Indexes()
	cursor, err := idx.List(context.TODO())
	if err != nil {
		return fmt.Errorf("failed to list indices: %s", err)
	}

	var results []bson.M
	if err = cursor.All(context.TODO(), &results); err != nil {
		return fmt.Errorf("failed to retrieve index list: %s", err)
	}

	for _, rec := range results {
		if recKeys, ok := rec["key"].(bson.M); ok {
			if _, hasLabel := recKeys[FIELD_LABEL]; hasLabel {
				if _, hasField := recKeys[field]; hasField {
					if partialFilter, ok := rec["partialFilterExpression"].(bson.M); ok {
						if partialLabel, ok := partialFilter[FIELD_LABEL].(string); ok && partialLabel == label {
							log.Debugln("HELLO ", partialLabel)
							if _, err := idx.DropOne(context.TODO(), rec["name"].(string)); err != nil {
								return fmt.Errorf("failed to delete index for label %s on field %s: %s", label, field, err)
							}
							return nil
						}
					}
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
		idx := mg.ar.VertexCollection(mg.graph).Indexes()
		cursor, err := idx.List(context.TODO())
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetVertexIndexList: failed to list indices")
			return
		}

		var idxList []bson.M
		if err = cursor.All(context.TODO(), &idxList); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetVertexIndexList: failed to retrieve index list")
			return
		}

		for _, rec := range idxList {
			if recKeys, ok := rec["key"].(bson.M); ok {
				if _, hasLabelKey := recKeys[FIELD_LABEL]; hasLabelKey {
					for key := range recKeys {
						if key != FIELD_LABEL {
							f := strings.TrimPrefix(key, "data.")
							if partialFilter, ok := rec["partialFilterExpression"].(bson.M); ok {
								log.Debugln("HELLO2 ", partialFilter)
								if label, ok := partialFilter[FIELD_LABEL].(string); ok {
									out <- &gripql.IndexID{Graph: mg.graph, Label: label, Field: f}
								}
							}
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
			FIELD_LABEL: label,
		}
		vcol := mg.ar.VertexCollection(mg.graph)
		opts := options.Find()
		opts.SetProjection(map[string]interface{}{FIELD_ID: 1, FIELD_LABEL: 1})

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
					out <- result[FIELD_ID].(string)
				}
			}
			if err := cursor.Close(context.TODO()); err != nil {
				log.Errorln("VertexLabelScan error:", err)
			}
		}
	}()
	return out
}
