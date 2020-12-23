package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"google.golang.org/protobuf/types/known/structpb"
	"github.com/bmeg/grip/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

// BuildSchema returns the schema of a specific graph in the database
func (ma *GraphDB) BuildSchema(ctx context.Context, graph string, sampleN uint32, random bool) (*gripql.Graph, error) {
	var vSchema []*gripql.Vertex
	var eSchema []*gripql.Edge
	var g errgroup.Group

	start := time.Now()
	log.WithFields(log.Fields{"graph": graph}).Debug("Starting BuildSchema call")

	g.Go(func() error {
		var err error
		vSchema, err = ma.getVertexSchema(ctx, graph, sampleN, random)
		if err != nil {
			return fmt.Errorf("getting vertex schema: %v", err)
		}
		return nil
	})

	g.Go(func() error {
		var err error
		eSchema, err = ma.getEdgeSchema(ctx, graph, sampleN, random)
		if err != nil {
			return fmt.Errorf("getting edge schema: %v", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	schema := &gripql.Graph{Graph: graph, Vertices: vSchema, Edges: eSchema}
	log.WithFields(log.Fields{"graph": graph, "elapsed_time": time.Since(start).String()}).Debug("Finished BuildSchema call")
	return schema, nil
}

func (ma *GraphDB) getVertexSchema(ctx context.Context, graph string, n uint32, random bool) ([]*gripql.Vertex, error) {

	gr, _ := ma.Graph(graph)
	labels, err := gr.ListVertexLabels()
	if err != nil {
		return nil, err
	}

	schemaChan := make(chan *gripql.Vertex)
	var g errgroup.Group

	for _, label := range labels {
		label := label
		if label == "" {
			continue
		}
		g.Go(func() error {
			start := time.Now()
			log.WithFields(log.Fields{"graph": graph, "label": label}).Debug("getVertexSchema: Started schema build")

			pipe := []bson.M{
				{
					"$match": bson.M{
						"label": bson.M{"$eq": label},
					},
				},
			}

			if random {
				pipe = append(pipe, bson.M{"$sample": bson.M{"size": n}})
			} else {
				pipe = append(pipe, bson.M{"$limit": n})
			}

			cursor, _ := ma.VertexCollection(graph).Aggregate(context.TODO(), pipe)
			result := make(map[string]interface{})
			schema := make(map[string]interface{})
			for cursor.Next(context.TODO()) {
				select {
				case <-ctx.Done():
					return ctx.Err()

				default:
					if err := cursor.Decode(&result); err == nil {
						if result["data"] != nil {
							ds := gripql.GetDataFieldTypes(result["data"].(map[string]interface{}))
							util.MergeMaps(schema, ds)
						}
					} else {
						log.WithFields(log.Fields{"graph": graph, "label": label, "error": err}).Error("getVertexSchema: bad vertex")
					}
				}
			}
			if err := cursor.Close(context.TODO()); err != nil {
				log.WithFields(log.Fields{"graph": graph, "label": label, "error": err}).Error("getVertexSchema: MongoDB: iter error")
				return err
			}
			sSchema, _ := structpb.NewStruct(schema)
			vSchema := &gripql.Vertex{Gid: label, Label: label, Data: sSchema}
			schemaChan <- vSchema
			log.WithFields(log.Fields{"graph": graph, "label": label, "elapsed_time": time.Since(start).String()}).Debug("getVertexSchema: Finished schema build")
			return nil
		})
	}

	output := []*gripql.Vertex{}
	done := make(chan interface{})
	go func() {
		for s := range schemaChan {
			output = append(output, s)
		}
		close(done)
	}()

	err = g.Wait()
	close(schemaChan)
	<-done
	return output, err
}

func (ma *GraphDB) getEdgeSchema(ctx context.Context, graph string, n uint32, random bool) ([]*gripql.Edge, error) {

	gr, _ := ma.Graph(graph)
	labels, err := gr.ListEdgeLabels()
	if err != nil {
		return nil, err
	}

	schemaChan := make(chan *gripql.Edge)
	var g errgroup.Group

	for _, label := range labels {
		label := label
		if label == "" {
			continue
		}
		g.Go(func() error {
			start := time.Now()
			log.WithFields(log.Fields{"graph": graph, "label": label}).Debug("getEdgeSchema: Started schema build")

			pipe := []bson.M{
				{
					"$match": bson.M{
						"label": bson.M{"$eq": label},
					},
				},
			}

			if random {
				pipe = append(pipe, bson.M{"$sample": bson.M{"size": n}})
			} else {
				pipe = append(pipe, bson.M{"$limit": n})
			}

			cursor, _ := ma.EdgeCollection(graph).Aggregate(context.TODO(), pipe)
			defer cursor.Close(context.TODO())
			result := make(map[string]interface{})
			schema := make(map[string]interface{})
			fromToPairs := make(fromto)

			for cursor.Next(context.TODO()) {
				select {
				case <-ctx.Done():
					return ctx.Err()

				default:
					if err := cursor.Decode(&result); err == nil {
						fromToPairs.Add(fromtokey{result["from"].(string), result["to"].(string)})
						if result["data"] != nil {
							ds := gripql.GetDataFieldTypes(result["data"].(map[string]interface{}))
							util.MergeMaps(schema, ds)
						}
					} else {
						log.WithFields(log.Fields{"graph": graph, "label": label, "error": err}).Error("getVertexSchema: bad vertex")
					}
				}
			}
			if err := cursor.Close(context.TODO()); err != nil {
				log.WithFields(log.Fields{"graph": graph, "label": label, "error": err}).Error("getEdgeSchema: MongoDB: iter error")
				return err
			}

			fromToPairs = ma.resolveLabels(graph, fromToPairs)
			from := fromToPairs.GetFrom()
			to := fromToPairs.GetTo()

			for j := 0; j < len(from); j++ {
				sSchema, _ := structpb.NewStruct(schema)
				eSchema := &gripql.Edge{
					Gid:   fmt.Sprintf("(%s)--%s->(%s)", from[j], label, to[j]),
					Label: label,
					From:  from[j],
					To:    to[j],
					Data:  sSchema,
				}
				schemaChan <- eSchema
			}
			log.WithFields(log.Fields{"graph": graph, "label": label, "elapsed_time": time.Since(start).String()}).Debug("getEdgeSchema: Finished schema build")

			return nil
		})
	}

	output := []*gripql.Edge{}
	done := make(chan interface{})
	go func() {
		for s := range schemaChan {
			output = append(output, s)
		}
		close(done)
	}()

	err = g.Wait()
	close(schemaChan)
	<-done
	return output, err
}

type fromtokey struct {
	from, to string
}

type fromto map[fromtokey]interface{}

func (ft fromto) Add(k fromtokey) bool {
	if k.from != "" && k.to != "" {
		// only keep if both from and to labels are valid
		ft[k] = nil
		return true
	}
	return false
}

func (ft fromto) GetFrom() []string {
	out := []string{}
	for k := range ft {
		out = append(out, k.from)
	}
	return out
}

func (ft fromto) GetTo() []string {
	out := []string{}
	for k := range ft {
		out = append(out, k.to)
	}
	return out
}

func (ma *GraphDB) resolveLabels(graph string, ft fromto) fromto {
	out := make([]fromtokey, len(ft))
	var g errgroup.Group

	fromIDs := ft.GetFrom()
	toIDs := ft.GetTo()

	for i := 0; i < len(fromIDs); i++ {
		i := i
		toID := toIDs[i]
		fromID := fromIDs[i]

		g.Go(func() error {
			v := ma.VertexCollection(graph)
			from := ""
			to := ""
			result := map[string]string{}
			opts := options.Find()
			opts.SetProjection(bson.M{"_id": -1, "label": 1})
			cursor := v.FindOne(context.TODO(), bson.M{"_id": fromID})
			if cursor.Err() == nil {
				if nil == cursor.Decode(&result) {
					from = result["label"]
				}
			}
			result = map[string]string{}
			cursor = v.FindOne(context.TODO(), bson.M{"_id": toID})
			if cursor.Err() == nil {
				if nil == cursor.Decode(&result) {
					to = result["label"]
				}
			}
			if from != "" && to != "" {
				out[i] = fromtokey{from, to}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil
	}

	outMap := make(fromto)
	for _, k := range out {
		outMap.Add(k)
	}

	return outMap
}
