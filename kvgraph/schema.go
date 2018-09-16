package kvgraph

import (
	//"fmt"
	"context"

	"github.com/bmeg/grip/gripql"
)

func (kgraph *KVGraph) GetSchema(ctx context.Context, graph string, sampleN uint32, random bool) (*gripql.GraphSchema, error) {
	return nil, nil
}

/*
// GetSchema returns the schema of a specific graph in the database
func (kgraph *KVGraph) GetSchema(ctx context.Context, graph string, sampleN uint32, random bool) (*gripql.GraphSchema, error) {
  var vSchema []*gripql.Vertex
	var eSchema []*gripql.Edge
	var g errgroup.Group

	g.Go(func() error {
		var err error
		vSchema, err = kgraph.getVertexSchema(ctx, graph, sampleN, random)
		if err != nil {
			return fmt.Errorf("getting vertex schema: %v", err)
		}
		return nil
	})

	g.Go(func() error {
		var err error
		eSchema, err = kgraph.getEdgeSchema(ctx, graph, sampleN, random)
		if err != nil {
			return fmt.Errorf("getting edge schema: %v", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	schema := &gripql.GraphSchema{Vertices: vSchema, Edges: eSchema}
	// log.Printf("Graph schema: %+v", schema)
	return schema, nil
}

func (ma *KVGraph) getVertexSchema(ctx context.Context, graph string, n uint32, random bool) ([]*gripql.Vertex, error) {
  // get distinct labels

	schemaChan := make(chan *gripql.Vertex)
	var g errgroup.Group

  // for each label,
	for _, label := range labels {
		label := label
		if label == "" {
			continue
		}
		g.Go(func() error {
			log.Printf("vertex label: %s: starting schema build", label)

			session := ma.session.Copy()
			err := session.Ping()
			if err != nil {
				log.Printf("session ping error: %v", err)
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
						ds := GetDataFieldTypes(result["data"].(map[string]interface{}))
						MergeMaps(schema, ds)
					}
				}
			}
			if err := iter.Close(); err != nil {
				err = fmt.Errorf("iter error building schema for label %s: %v", label, err)
				log.Printf(err.Error())
				return err
			}

			vSchema := &gripql.Vertex{Label: label, Data: protoutil.AsStruct(schema)}
			schemaChan <- vSchema
			log.Printf("vertex label: %s: finished schema build", label)

			return nil
		})
	}

	output := []*gripql.Vertex{}
	done := make(chan interface{})
	go func() {
		for s := range schemaChan {
			// log.Printf("Vertex schema: %+v", s)
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
			log.Printf("edge label: %s: starting schema build", label)

			session := ma.session.Copy()
			err := session.Ping()
			if err != nil {
				log.Printf("session ping error: %v", err)
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
						ds := GetDataFieldTypes(result["data"].(map[string]interface{}))
						MergeMaps(schema, ds)
					}
				}
			}
			if err := iter.Close(); err != nil {
				err = fmt.Errorf("iter error building schema for label %s: %v", label, err)
				log.Printf(err.Error())
				return err
			}

			fromToPairs = ma.resolveLabels(graph, fromToPairs)
			from := fromToPairs.GetFrom()
			to := fromToPairs.GetTo()

			for j := 0; j < len(from); j++ {
				eSchema := &gripql.Edge{Label: label, From: from[j], To: to[j], Data: protoutil.AsStruct(schema)}
				schemaChan <- eSchema
			}
			log.Printf("edge label: %s: finished schema build", label)

			return nil
		})
	}

	output := []*gripql.Edge{}
	done := make(chan interface{})
	go func() {
		for s := range schemaChan {
			// log.Printf("Edge schema: %+v", s)
			output = append(output, s)
		}
		close(done)
	}()

	err = g.Wait()
	close(schemaChan)
	<-done
	return output, err
}
*/
