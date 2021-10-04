package psql

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/gripql"
	gripSchema "github.com/bmeg/grip/gripql/schema"
	"google.golang.org/protobuf/types/known/structpb"
)

// BuildSchema returns the schema of a specific graph in the database
func (db *GraphDB) BuildSchema(ctx context.Context, graphID string, sampleN uint32, random bool) (*gripql.Graph, error) {


	fmt.Printf("Starting postgres schema check\n")
	var g errgroup.Group

	gi, err := db.Graph(graphID)
	if err != nil {
		return nil, err
	}

	graph := gi.(*Graph)

	schemaChan := make(chan *gripql.Vertex)

	vLabels, err := graph.ListVertexLabels()
	if err != nil {
		return nil, err
	}

	for _, label := range vLabels {
		label := label
		if label == "" {
			continue
		}
		g.Go(func() error {
			q := fmt.Sprintf("SELECT * FROM %s WHERE label='%s'", graph.v, label)
			rows, err := graph.db.QueryxContext(ctx, q)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("BuildSchema: QueryxContext")
				return err
			}
			defer rows.Close()
			schema := make(map[string]interface{})
			for rows.Next() {
				vrow := &row{}
				if err := rows.StructScan(vrow); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("BuildSchema: StructScan")
					continue
				}
				v, err := convertVertexRow(vrow, true)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("BuildSchema: convertVertexRow")
					continue
				}
				gripSchema.MergeMaps(schema, v.Data)
			}

			sSchema, _ := structpb.NewStruct(schema)
			vSchema := &gripql.Vertex{Gid: label, Label: label, Data: sSchema}
			schemaChan <- vSchema

			return nil
		})
	}

	eLabels, err := graph.ListEdgeLabels()
	if err != nil {
		return nil, err
	}

	for _, label := range eLabels {
		label := label
		if label == "" {
			continue
		}

		g.Go(func() error {
			q := fmt.Sprintf(
				"SELECT a.label, b.label, c.label, b.data FROM %s as a INNER JOIN %s as b ON b.to=a.gid INNER JOIN %s as c on b.from = c.gid WHERE b.label = '%s' limit %d",
				graph.v, graph.e, graph.v,
				label, sampleN,
			)
			fmt.Printf("Query: %s\n", q)
			rows, err := graph.db.QueryxContext(ctx, q)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("BuildSchema: QueryxContext")
				return err
			}
			defer rows.Close()
			//schema := make(map[string]interface{})
			for rows.Next() {
				if row, err := rows.SliceScan(); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("BuildSchema: SliceScan")
					continue
				} else {
					fmt.Printf("Found: %s\n", row)
				}
			}
			return nil
		})

	}

	wg := errgroup.Group{}

	vSchema := []*gripql.Vertex{}
	eSchema := []*gripql.Edge{}

	wg.Go(func() error {
		for s := range schemaChan {
			vSchema = append(vSchema, s)
		}
		return nil
	})

	g.Wait()
 	close(schemaChan)

	wg.Wait()

	schema := &gripql.Graph{Graph: graphID, Vertices: vSchema, Edges: eSchema}
	return schema, nil
}
