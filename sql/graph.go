package sql

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/engine/core"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/timestamp"
	"github.com/jmoiron/sqlx"
)

// Graph is the interface to a single graph
type Graph struct {
	db     *sqlx.DB
	ts     *timestamp.Timestamp
	graph  string
	schema *Schema
}

// Compiler returns a query compiler that uses the graph
func (g *Graph) Compiler() gdbi.Compiler {
	return core.NewCompiler(g)
}

////////////////////////////////////////////////////////////////////////////////
// Write methods
////////////////////////////////////////////////////////////////////////////////

// AddVertex is not implemented in the SQL driver
func (g *Graph) AddVertex(vertexArray []*aql.Vertex) error {
	return errors.New("not implemented")
}

// AddEdge is not implemented in the SQL driver
func (g *Graph) AddEdge(edgeArray []*aql.Edge) error {
	return errors.New("not implemented")
}

// DelVertex is not implemented in the SQL driver
func (g *Graph) DelVertex(key string) error {
	return errors.New("not implemented")
}

// DelEdge is not implemented in the SQL driver
func (g *Graph) DelEdge(key string) error {
	return errors.New("not implemented")
}

////////////////////////////////////////////////////////////////////////////////
// Read methods
////////////////////////////////////////////////////////////////////////////////

// GetTimestamp gets the timestamp of last update
func (g *Graph) GetTimestamp() string {
	return g.ts.Get(g.graph)
}

// GetVertex loads a vertex given an id. It returns a nil if not found.
// Keys are expected to be of the form: <table>:<primary_key>
func (g *Graph) GetVertex(key string, load bool) *aql.Vertex {
	parts := strings.SplitN(key, ":", 2)
	if len(parts) != 2 {
		return nil
	}
	table := parts[0]
	id := parts[1]
	gidField := g.schema.GetVertexGid(table)
	q := fmt.Sprintf("SELECT * FROM %s WHERE %s=%s", table, gidField, id)
	data := make(map[string]interface{})
	err := g.db.QueryRowx(q).MapScan(data)
	if err != nil {
		return nil
	}
	return rowDataToVertex(table, g.schema, data, load)
}

func (g *Graph) getGeneratedEdge(key string, load bool) *aql.Edge {
	geid, err := parseGeneratedEdgeID(key)
	if err != nil {
		return nil
	}
	return &aql.Edge{
		Gid:   key,
		Label: geid.Label,
		From:  geid.FromID,
		To:    geid.ToID,
		Data:  nil,
	}
}

func (g *Graph) getTableBackedEdge(key string, load bool) *aql.Edge {
	parts := strings.SplitN(key, ":", 2)
	if len(parts) != 2 {
		return nil
	}
	table := parts[0]
	id := parts[1]
	gidField := g.schema.GetEdgeGid(table)
	q := fmt.Sprintf("SELECT * FROM %s WHERE %s=%s", table, gidField, id)
	data := make(map[string]interface{})
	err := g.db.QueryRowx(q).MapScan(data)
	if err != nil {
		return nil
	}
	return rowDataToEdge(table, g.schema, data, load)
}

// GetEdge loads an edge given an id. It returns nil if not found
// Keys are expected to be of the form: <table>:<primary_key>
func (g *Graph) GetEdge(key string, load bool) *aql.Edge {
	parts := strings.SplitN(key, ":", 2)
	if len(parts) != 2 {
		return nil
	}
	table := parts[0]
	if table == "generated" {
		return g.getGeneratedEdge(key, load)
	}
	return g.getTableBackedEdge(key, load)
}

// GetVertexList produces a channel of all vertices in the graph
func (g *Graph) GetVertexList(ctx context.Context, load bool) <-chan *aql.Vertex {
	o := make(chan *aql.Vertex, 100)
	go func() {
		defer close(o)
		for _, v := range g.schema.Vertices {
			q := fmt.Sprintf("SELECT * FROM %s", v.Table)
			rows, err := g.db.QueryxContext(ctx, q)
			if err != nil {
				log.Println("GetVertexList failed:", err)
				return
			}
			defer rows.Close()
			for rows.Next() {
				data := make(map[string]interface{})
				if err := rows.MapScan(data); err != nil {
					log.Println("GetVertexList failed:", err)
					return
				}
				o <- rowDataToVertex(v.Table, g.schema, data, load)
			}
			if err := rows.Err(); err != nil {
				log.Println("GetVertexList failed:", err)
				return
			}
		}
	}()
	return o
}

// VertexLabelScan produces a channel of all vertex ids where the vertex label matches `label`
func (g *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	o := make(chan string, 100)
	go func() {
		defer close(o)
		for _, v := range g.schema.Vertices {
			if v.Label == label {
				q := fmt.Sprintf("SELECT * FROM %s", v.Table)
				rows, err := g.db.QueryxContext(ctx, q)
				if err != nil {
					log.Println("VertexLabelScan failed:", err)
					return
				}
				defer rows.Close()
				for rows.Next() {
					data := make(map[string]interface{})
					if err := rows.MapScan(data); err != nil {
						log.Println("VertexLabelScan failed:", err)
						return
					}
					v := rowDataToVertex(v.Table, g.schema, data, false)
					o <- v.Gid
				}
				if err := rows.Err(); err != nil {
					log.Println("VertexLabelScan failed:", err)
					return
				}
			}
		}
	}()
	return o
}

// GetEdgeList produces a channel of all edges in the graph
func (g *Graph) GetEdgeList(ctx context.Context, load bool) <-chan *aql.Edge {
	o := make(chan *aql.Edge, 100)
	go func() {
		defer close(o)
		for _, edgeSchema := range g.schema.Edges {
			q := ""
			switch edgeSchema.Table {
			case "":
				q = fmt.Sprintf("SELECT %s.%s, %s.%s FROM %s INNER JOIN %s ON %s.%s=%s.%s",
					// SELECT
					edgeSchema.From.DestTable, g.schema.GetVertexGid(edgeSchema.From.DestTable),
					edgeSchema.To.DestTable, g.schema.GetVertexGid(edgeSchema.To.DestTable),
					// FROM
					edgeSchema.From.DestTable,
					// INNER JOIN
					edgeSchema.To.DestTable,
					// ON
					edgeSchema.From.DestTable, edgeSchema.From.DestField,
					edgeSchema.To.DestTable, edgeSchema.To.DestField,
				)
				rows, err := g.db.QueryxContext(ctx, q)
				if err != nil {
					log.Println("GetEdgeList failed:", err)
					return
				}
				defer rows.Close()
				for rows.Next() {
					var fromGid, toGid string
					if err := rows.Scan(&fromGid, &toGid); err != nil {
						log.Println("GetEdgeList failed:", err)
						return
					}
					geid := &generatedEdgeID{edgeSchema.Label, edgeSchema.From.DestTable, fromGid, edgeSchema.To.DestTable, toGid}
					edge := &aql.Edge{
						Gid:   geid.String(),
						Label: edgeSchema.Label,
						From:  fromGid,
						To:    toGid,
						Data:  nil,
					}
					o <- edge
				}
				if err := rows.Err(); err != nil {
					log.Println("GetEdgeList failed:", err)
					return
				}

			default:
				q = fmt.Sprintf("SELECT * FROM %s", edgeSchema.Table)
				rows, err := g.db.QueryxContext(ctx, q)
				if err != nil {
					log.Println("GetEdgeList failed:", err)
					return
				}
				defer rows.Close()
				for rows.Next() {
					data := make(map[string]interface{})
					if err := rows.MapScan(data); err != nil {
						log.Println("GetEdgeList failed:", err)
						return
					}
					o <- rowDataToEdge(edgeSchema.Table, g.schema, data, load)
				}
				if err := rows.Err(); err != nil {
					log.Println("GetEdgeList failed:", err)
					return
				}
			}
		}
	}()
	return o
}

// GetVertexChannel is passed a channel of vertex ids and it produces a channel
// of vertices
func (g *Graph) GetVertexChannel(reqChan chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	batches := make(map[string][]gdbi.ElementLookup)
	for elem := range reqChan {
		parts := strings.SplitN(elem.ID, ":", 2)
		if len(parts) != 2 {
			log.Println("GetVertexChannel encountered a strange ID:", elem.ID)
			continue
		}
		table := parts[0]
		batches[table] = append(batches[table], elem)
	}

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for table, batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				parts := strings.SplitN(batch[i].ID, ":", 2)
				if len(parts) != 2 {
					log.Println("GetVertexChannel encountered a strange ID:", batch[i].ID)
					continue
				}
				idBatch[i] = parts[1]
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			ids := strings.Join(idBatch, ", ")
			gidField := g.schema.GetVertexGid(table)
			q := fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s)", table, gidField, ids)
			rows, err := g.db.Queryx(q)
			if err != nil {
				log.Println("GetVertexChannel failed:", err)
				return
			}
			defer rows.Close()
			for rows.Next() {
				data := make(map[string]interface{})
				if err := rows.MapScan(data); err != nil {
					log.Println("GetVertexChannel failed:", err)
					return
				}
				v := rowDataToVertex(table, g.schema, data, load)
				r := batchMap[v.Gid]
				for _, ri := range r {
					ri.Vertex = v
					o <- ri
				}
			}
			if err := rows.Err(); err != nil {
				log.Println("GetVertexChannel failed:", err)
				return
			}
		}
	}()

	return o
}

// GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (g *Graph) GetOutChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(map[string][]gdbi.ElementLookup)
	for elem := range reqChan {
		parts := strings.SplitN(elem.ID, ":", 2)
		if len(parts) != 2 {
			log.Println("GetOutChannel encountered a strange ID:", elem.ID)
			continue
		}
		table := parts[0]
		batches[table] = append(batches[table], elem)
	}

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for table, batch := range batches {
			idBatch := []string{}
			batchMap := make(map[string][]gdbi.ElementLookup)
			for i := range batch {
				parts := strings.SplitN(batch[i].ID, ":", 2)
				if len(parts) != 2 {
					log.Println("GetOutChannel encountered a strange ID:", batch[i].ID)
					continue
				}
				idBatch = append(idBatch, parts[1])
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			ids := strings.Join(idBatch, ", ")
			outgoingEdges := g.schema.GetOutgoingEdges(table, edgeLabels)
			for _, edgeSchema := range outgoingEdges {
				q := ""
				dataKey := ""
				switch edgeSchema.Table {
				case "":
					q = fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s)",
						// FROM
						edgeSchema.To.DestTable,
						// WHERE
						edgeSchema.To.DestField,
						ids,
					)
					dataKey = edgeSchema.From.DestField
				default:
					q = fmt.Sprintf("SELECT %s.%s, %s.%s FROM %s INNER JOIN %s ON %s.%s=%s.%s WHERE %s.%s IN (%s)",
						// SELECT
						edgeSchema.To.DestTable, "*",
						edgeSchema.Table, edgeSchema.From.SourceField,
						// FROM
						edgeSchema.To.DestTable,
						// INNER JOIN
						edgeSchema.Table,
						// ON
						edgeSchema.To.DestTable, edgeSchema.To.DestField,
						edgeSchema.Table, edgeSchema.To.SourceField,
						// WHERE
						edgeSchema.Table, edgeSchema.From.SourceField,
						ids,
					)
					dataKey = edgeSchema.From.SourceField
				}
				rows, err := g.db.Queryx(q)
				if err != nil {
					log.Println("GetOutChannel failed:", err)
					return
				}
				defer rows.Close()
				for rows.Next() {
					data := make(map[string]interface{})
					if err := rows.MapScan(data); err != nil {
						log.Println("GetOutChannel failed:", err)
						return
					}
					v := rowDataToVertex(edgeSchema.To.DestTable, g.schema, data, load)
					r := batchMap[fmt.Sprintf("%v:%v", edgeSchema.From.DestTable, data[dataKey])]
					for _, ri := range r {
						ri.Vertex = v
						o <- ri
					}
				}
				if err := rows.Err(); err != nil {
					log.Println("GetOutChannel failed:", err)
					return
				}
			}
		}
	}()

	return o
}

// GetInChannel process requests of vertex ids and find the connected vertices on incoming edges
func (g *Graph) GetInChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(map[string][]gdbi.ElementLookup)
	for elem := range reqChan {
		parts := strings.SplitN(elem.ID, ":", 2)
		if len(parts) != 2 {
			log.Println("GetInChannel encountered a strange ID:", elem.ID)
			continue
		}
		table := parts[0]
		batches[table] = append(batches[table], elem)
	}

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for table, batch := range batches {
			idBatch := []string{}
			batchMap := make(map[string][]gdbi.ElementLookup)
			for i := range batch {
				parts := strings.SplitN(batch[i].ID, ":", 2)
				if len(parts) != 2 {
					log.Println("GetInChannel encountered a strange ID:", batch[i].ID)
					continue
				}
				idBatch = append(idBatch, parts[1])
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			ids := strings.Join(idBatch, ", ")
			incomingEdges := g.schema.GetIncomingEdges(table, edgeLabels)
			for _, edgeSchema := range incomingEdges {
				q := ""
				dataKey := ""
				switch edgeSchema.Table {
				case "":
					q = fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s)",
						// FROM
						edgeSchema.From.DestTable,
						// WHERE
						edgeSchema.From.DestField,
						ids,
					)
					dataKey = edgeSchema.From.DestField
				default:
					q = fmt.Sprintf("SELECT %s.%s, %s.%s FROM %s INNER JOIN %s ON %s.%s=%s.%s WHERE %s.%s IN (%s)",
						// SELECT
						edgeSchema.From.DestTable, "*",
						edgeSchema.Table, edgeSchema.To.SourceField,
						// FROM
						edgeSchema.From.DestTable,
						// INNER JOIN
						edgeSchema.Table,
						// ON
						edgeSchema.From.DestTable, edgeSchema.From.DestField,
						edgeSchema.Table, edgeSchema.From.SourceField,
						// WHERE
						edgeSchema.Table, edgeSchema.To.SourceField,
						ids,
					)
					dataKey = edgeSchema.To.SourceField
				}
				rows, err := g.db.Queryx(q)
				if err != nil {
					log.Println("GetInChannel failed:", err)
					return
				}
				defer rows.Close()
				for rows.Next() {
					data := make(map[string]interface{})
					if err := rows.MapScan(data); err != nil {
						log.Println("GetInChannel failed:", err)
						return
					}
					v := rowDataToVertex(edgeSchema.From.DestTable, g.schema, data, load)
					r := batchMap[fmt.Sprintf("%v:%v", edgeSchema.To.DestTable, data[dataKey])]
					for _, ri := range r {
						ri.Vertex = v
						o <- ri
					}
				}
				if err := rows.Err(); err != nil {
					log.Println("GetInChannel failed:", err)
					return
				}
			}
		}
	}()

	return o
}

// GetOutEdgeChannel process requests of vertex ids and find the connected outgoing edges
func (g *Graph) GetOutEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(map[string][]gdbi.ElementLookup)
	for elem := range reqChan {
		parts := strings.SplitN(elem.ID, ":", 2)
		if len(parts) != 2 {
			log.Println("GetOutEdgeChannel encountered a strange ID:", elem.ID)
			continue
		}
		table := parts[0]
		batches[table] = append(batches[table], elem)
	}

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for table, batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				parts := strings.SplitN(batch[i].ID, ":", 2)
				if len(parts) != 2 {
					log.Println("GetOutEdgeChannel encountered a strange ID:", batch[i].ID)
					continue
				}
				idBatch[i] = parts[1]
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			ids := strings.Join(idBatch, ", ")
			outgoingEdges := g.schema.GetOutgoingEdges(table, edgeLabels)
			for _, edgeSchema := range outgoingEdges {
				q := ""
				switch edgeSchema.Table {
				case "":
					q = fmt.Sprintf("SELECT %s.%s, %s.%s FROM %s INNER JOIN %s ON %s.%s=%s.%s WHERE %s.%s IN (%s)",
						// SELECT
						edgeSchema.From.DestTable, g.schema.GetVertexGid(edgeSchema.From.DestTable),
						edgeSchema.To.DestTable, g.schema.GetVertexGid(edgeSchema.To.DestTable),
						// FROM
						edgeSchema.From.DestTable,
						// INNER JOIN
						edgeSchema.To.DestTable,
						// ON
						edgeSchema.From.DestTable, edgeSchema.From.DestField,
						edgeSchema.To.DestTable, edgeSchema.To.DestField,
						// WHERE
						edgeSchema.From.DestTable, g.schema.GetVertexGid(edgeSchema.From.DestTable),
						ids,
					)
					rows, err := g.db.Queryx(q)
					if err != nil {
						log.Println("GetOutEdgeChannel failed:", err)
						return
					}
					defer rows.Close()
					for rows.Next() {
						var fromGid, toGid string
						if err := rows.Scan(&fromGid, &toGid); err != nil {
							log.Println("GetOutEdgeChannel failed:", err)
							return
						}
						geid := &generatedEdgeID{edgeSchema.Label, edgeSchema.From.DestTable, fromGid, edgeSchema.To.DestTable, toGid}
						edge := &aql.Edge{
							Gid:   geid.String(),
							Label: edgeSchema.Label,
							From:  fromGid,
							To:    toGid,
							Data:  nil,
						}
						r := batchMap[fmt.Sprintf("%v:%v", edgeSchema.From.DestTable, fromGid)]
						for _, ri := range r {
							ri.Edge = edge
							o <- ri
						}
					}
					if err := rows.Err(); err != nil {
						log.Println("GetOutEdgeChannel failed:", err)
						return
					}

				default:
					q = fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s)", edgeSchema.Table, edgeSchema.From.SourceField, ids)
					rows, err := g.db.Queryx(q)
					if err != nil {
						log.Println("GetOutEdgeChannel failed:", err)
						return
					}
					defer rows.Close()
					for rows.Next() {
						data := make(map[string]interface{})
						if err := rows.MapScan(data); err != nil {
							log.Println("GetOutEdgeChannel failed:", err)
							return
						}
						edge := rowDataToEdge(edgeSchema.Table, g.schema, data, load)
						r := batchMap[fmt.Sprintf("%v:%v", edgeSchema.From.DestTable, data[edgeSchema.From.SourceField])]
						for _, ri := range r {
							ri.Edge = edge
							o <- ri
						}
					}
					if err := rows.Err(); err != nil {
						log.Println("GetOutEdgeChannel failed:", err)
						return
					}
				}
			}
		}
	}()
	return o
}

// GetInEdgeChannel process requests of vertex ids and find the connected incoming edges
func (g *Graph) GetInEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(map[string][]gdbi.ElementLookup)
	for elem := range reqChan {
		parts := strings.SplitN(elem.ID, ":", 2)
		if len(parts) != 2 {
			log.Println("GetInEdgeChannel encountered a strange ID:", elem.ID)
			continue
		}
		table := parts[0]
		batches[table] = append(batches[table], elem)
	}

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for table, batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				parts := strings.SplitN(batch[i].ID, ":", 2)
				if len(parts) != 2 {
					log.Println("GetInEdgeChannel encountered a strange ID:", batch[i].ID)
					continue
				}
				idBatch[i] = parts[1]
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			ids := strings.Join(idBatch, ", ")
			incomingEdges := g.schema.GetIncomingEdges(table, edgeLabels)
			for _, edgeSchema := range incomingEdges {
				q := ""
				switch edgeSchema.Table {
				case "":
					q = fmt.Sprintf("SELECT %s.%s, %s.%s FROM %s INNER JOIN %s ON %s.%s=%s.%s WHERE %s.%s IN (%s)",
						// SELECT
						edgeSchema.From.DestTable, g.schema.GetVertexGid(edgeSchema.From.DestTable),
						edgeSchema.To.DestTable, g.schema.GetVertexGid(edgeSchema.To.DestTable),
						// FROM
						edgeSchema.From.DestTable,
						// INNER JOIN
						edgeSchema.To.DestTable,
						// ON
						edgeSchema.From.DestTable, edgeSchema.From.DestField,
						edgeSchema.To.DestTable, edgeSchema.To.DestField,
						// WHERE
						edgeSchema.To.DestTable, g.schema.GetVertexGid(edgeSchema.To.DestTable),
						ids,
					)
					rows, err := g.db.Queryx(q)
					if err != nil {
						log.Println("GetInEdgeChannel failed:", err)
						return
					}
					defer rows.Close()
					for rows.Next() {
						var fromGid, toGid string
						if err := rows.Scan(&fromGid, &toGid); err != nil {
							log.Println("GetInEdgeChannel failed:", err)
							return
						}
						geid := &generatedEdgeID{edgeSchema.Label, edgeSchema.From.DestTable, fromGid, edgeSchema.To.DestTable, toGid}
						edge := &aql.Edge{
							Gid:   geid.String(),
							Label: edgeSchema.Label,
							From:  fromGid,
							To:    toGid,
							Data:  nil,
						}
						r := batchMap[fmt.Sprintf("%v:%v", edgeSchema.To.DestTable, toGid)]
						for _, ri := range r {
							ri.Edge = edge
							o <- ri
						}
					}
					if err := rows.Err(); err != nil {
						log.Println("GetInEdgeChannel failed:", err)
						return
					}

				default:
					q = fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s)", edgeSchema.Table, edgeSchema.To.SourceField, ids)
					rows, err := g.db.Queryx(q)
					if err != nil {
						log.Println("GetInEdgeChannel failed:", err)
						return
					}
					defer rows.Close()
					for rows.Next() {
						data := make(map[string]interface{})
						if err := rows.MapScan(data); err != nil {
							log.Println("GetInEdgeChannel failed:", err)
							return
						}
						edge := rowDataToEdge(edgeSchema.Table, g.schema, data, load)
						r := batchMap[fmt.Sprintf("%v:%v", edgeSchema.To.DestTable, data[edgeSchema.To.SourceField])]
						for _, ri := range r {
							ri.Edge = edge
							o <- ri
						}
					}
					if err := rows.Err(); err != nil {
						log.Println("GetInEdgeChannel failed:", err)
						return
					}
				}
			}
		}
	}()
	return o
}
