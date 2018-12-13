package mongo

import (
	"context"
	"fmt"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/protoutil"
	"github.com/bmeg/grip/util"
	"github.com/globalsign/mgo/bson"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// GetSchema returns the schema of a specific graph in the database
func (ma *GraphDB) GetSchema(ctx context.Context, graph string, sampleN uint32, random bool) (*gripql.GraphSchema, error) {
	var vSchema []*gripql.Vertex
	var eSchema []*gripql.Edge
	var g errgroup.Group

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

	schema := &gripql.GraphSchema{Vertices: vSchema, Edges: eSchema}
	log.WithFields(log.Fields{"graph": graph}).Debug("Finished GetSchema call")
	return schema, nil
}

func (ma *GraphDB) getVertexSchema(ctx context.Context, graph string, n uint32, random bool) ([]*gripql.Vertex, error) {
	session := ma.session.Copy()
	defer session.Close()
	v := ma.VertexCollection(session, graph)

	var labels []string
	err := v.Find(nil).Distinct("label", &labels)
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
			log.WithFields(log.Fields{"graph": graph, "label": label}).Debug("getVertexSchema: Started schema build")

			session := ma.session.Copy()
			err := session.Ping()
			if err != nil {
				log.WithFields(log.Fields{"graph": graph, "label": label, "error": err}).Warning("getVertexSchema: Session ping error")
				session.Refresh()
			}
			defer session.Close()
			v := ma.VertexCollection(session, graph)

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

			iter := v.Pipe(pipe).AllowDiskUse().Iter()
			defer iter.Close()
			result := make(map[string]interface{})
			schema := make(map[string]interface{})
			for iter.Next(&result) {
				select {
				case <-ctx.Done():
					return ctx.Err()

				default:
					if result["data"] != nil {
						ds := gripql.GetDataFieldTypes(result["data"].(map[string]interface{}))
						util.MergeMaps(schema, ds)
					}
				}
			}
			if err := iter.Close(); err != nil {
				log.WithFields(log.Fields{"graph": graph, "label": label, "error": err}).Error("getVertexSchema: MongoDB: iter error")
				return err
			}

			vSchema := &gripql.Vertex{Label: label, Data: protoutil.AsStruct(schema)}
			schemaChan <- vSchema
			log.WithFields(log.Fields{"graph": graph, "label": label}).Debug("getVertexSchema: Finished schema build")

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
	session := ma.session.Copy()
	defer session.Close()
	e := ma.EdgeCollection(session, graph)

	var labels []string
	err := e.Find(nil).Distinct("label", &labels)
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
			log.WithFields(log.Fields{"graph": graph, "label": label}).Debug("getEdgeSchema: Started schema build")

			session := ma.session.Copy()
			err := session.Ping()
			if err != nil {
				log.WithFields(log.Fields{"graph": graph, "label": label, "error": err}).Warning("getEdgeSchema: Session ping error")
				session.Refresh()
			}
			defer session.Close()
			e := ma.EdgeCollection(session, graph)

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

			iter := e.Pipe(pipe).AllowDiskUse().Iter()
			defer iter.Close()
			result := make(map[string]interface{})
			schema := make(map[string]interface{})
			fromToPairs := make(fromto)

			for iter.Next(&result) {
				select {
				case <-ctx.Done():
					return ctx.Err()

				default:
					fromToPairs.Add(fromtokey{result["from"].(string), result["to"].(string)})
					if result["data"] != nil {
						ds := gripql.GetDataFieldTypes(result["data"].(map[string]interface{}))
						util.MergeMaps(schema, ds)
					}
				}
			}
			if err := iter.Close(); err != nil {
				log.WithFields(log.Fields{"graph": graph, "label": label, "error": err}).Error("getEdgeSchema: MongoDB: iter error")
				return err
			}

			fromToPairs = ma.resolveLabels(graph, fromToPairs)
			from := fromToPairs.GetFrom()
			to := fromToPairs.GetTo()

			for j := 0; j < len(from); j++ {
				eSchema := &gripql.Edge{Label: label, From: from[j], To: to[j], Data: protoutil.AsStruct(schema)}
				schemaChan <- eSchema
			}
			log.WithFields(log.Fields{"graph": graph, "label": label}).Debug("getEdgeSchema: Finished schema build")

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
			session := ma.session.Copy()
			defer session.Close()
			col := ma.VertexCollection(session, graph)

			from := ""
			to := ""
			result := map[string]string{}
			err := col.FindId(fromID).Select(bson.M{"_id": -1, "label": 1}).One(&result)
			if err == nil {
				from = result["label"]
			}
			result = map[string]string{}
			err = col.FindId(toID).Select(bson.M{"_id": -1, "label": 1}).One(&result)
			if err == nil {
				to = result["label"]
			}
			out[i] = fromtokey{from, to}
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
