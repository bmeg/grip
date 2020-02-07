package esql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/timestamp"
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
	return core.NewCompiler(g, core.IndexStartOptimize) //TODO: probably a better optimizer for vertex label search
}

////////////////////////////////////////////////////////////////////////////////
// Write methods
////////////////////////////////////////////////////////////////////////////////

// AddVertex is not implemented in the SQL driver
func (g *Graph) AddVertex(vertices []*gripql.Vertex) error {
	return errors.New("not implemented")
}

// AddEdge is not implemented in the SQL driver
func (g *Graph) AddEdge(edges []*gripql.Edge) error {
	return errors.New("not implemented")
}

func (g *Graph) BulkAdd(stream <-chan *gripql.GraphElement) error {
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
func (g *Graph) GetVertex(key string, load bool) *gripql.Vertex {
	parts := strings.SplitN(key, ":", 2)
	if len(parts) != 2 {
		return nil
	}
	table := parts[0]
	id := parts[1]
	gidField := g.schema.GetVertexGid(table)
	q := fmt.Sprintf("SELECT * FROM %s WHERE %s=%s", table, gidField, id)
	data := make(map[string]interface{})
	row := g.db.QueryRowx(q)
	types, err := rowColumnTypeMap(row)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("GetVertex: rowColumnTypeMap")
		return nil
	}
	err = row.MapScan(data)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("GetVertex: MapScan")
		return nil
	}
	res := rowDataToVertex(g.schema.GetVertex(table), data, types, load)
	return res
}

func (g *Graph) getGeneratedEdge(key string, load bool) *gripql.Edge {
	geid, err := parseGeneratedEdgeID(key)
	if err != nil {
		return nil
	}
	return geid.Edge()
}

func (g *Graph) getTableBackedEdge(key string, load bool) *gripql.Edge {
	parts := strings.SplitN(key, ":", 2)
	if len(parts) != 2 {
		return nil
	}
	table := parts[0]
	id := parts[1]
	edgeSchema := g.schema.GetEdge(table)
	gidField := edgeSchema.GidField
	q := fmt.Sprintf("SELECT * FROM %s WHERE %s=%s", table, gidField, id)
	data := make(map[string]interface{})
	row := g.db.QueryRowx(q)
	types, err := rowColumnTypeMap(row)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("GetEdge: rowColumnTypeMap")
		return nil
	}
	err = row.MapScan(data)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("GetEdge: MapScan")
		return nil
	}
	return rowDataToEdge(g.schema.GetEdge(table), data, types, load)
}

// GetEdge loads an edge given an id. It returns nil if not found
// Keys are expected to be of the form: <table>:<primary_key>
func (g *Graph) GetEdge(key string, load bool) *gripql.Edge {
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
func (g *Graph) GetVertexList(ctx context.Context, load bool) <-chan *gripql.Vertex {
	o := make(chan *gripql.Vertex, 100)
	go func() {
		defer close(o)
		for _, v := range g.schema.Vertices {
			q := fmt.Sprintf("SELECT * FROM %s", v.Table)
			rows, err := g.db.QueryxContext(ctx, q)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetVertexList: QueryxContext")
				return
			}
			types, err := columnTypeMap(rows)
			if err != nil {
				return
			}
			defer rows.Close()
			for rows.Next() {
				data := make(map[string]interface{})
				if err := rows.MapScan(data); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetVertexList: MapScan")
					return
				}
				o <- rowDataToVertex(v, data, types, load)
			}
			if err := rows.Err(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetVertexList: iterating")
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
					log.WithFields(log.Fields{"error": err}).Error("VertexLabelScan: QueryxContext")
					return
				}
				types, err := columnTypeMap(rows)
				if err != nil {
					return
				}
				defer rows.Close()
				for rows.Next() {
					data := make(map[string]interface{})
					if err := rows.MapScan(data); err != nil {
						log.WithFields(log.Fields{"error": err}).Error("VertexLabelScan: MapScan")
						log.Errorln("VertexLabelScan failed:", err)
						return
					}
					v := rowDataToVertex(v, data, types, false)
					o <- v.Gid
				}
				if err := rows.Err(); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("VertexLabelScan: iterating")
					return
				}
			}
		}
	}()
	return o
}

// GetEdgeList produces a channel of all edges in the graph
func (g *Graph) GetEdgeList(ctx context.Context, load bool) <-chan *gripql.Edge {
	o := make(chan *gripql.Edge, 100)
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
					log.WithFields(log.Fields{"error": err}).Error("GetEdgeList: QueryxContext")
					return
				}
				defer rows.Close()
				for rows.Next() {
					var fromGid, toGid string
					if err := rows.Scan(&fromGid, &toGid); err != nil {
						log.WithFields(log.Fields{"error": err}).Error("GetEdgeList: Scan")
						return
					}
					geid := &generatedEdgeID{edgeSchema.Label, edgeSchema.From.DestTable, fromGid, edgeSchema.To.DestTable, toGid}
					edge := geid.Edge()
					o <- edge
				}
				if err := rows.Err(); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetEdgeList: iterating")
					return
				}

			default:
				q = fmt.Sprintf("SELECT * FROM %s", edgeSchema.Table)
				rows, err := g.db.QueryxContext(ctx, q)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetEdgeList: QueryxContext")
					return
				}
				types, err := columnTypeMap(rows)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetEdgeList: columnTypeMap")
					return
				}

				defer rows.Close()
				for rows.Next() {
					data := make(map[string]interface{})
					if err := rows.MapScan(data); err != nil {
						log.WithFields(log.Fields{"error": err}).Error("GetEdgeList: MapScan")
						return
					}
					o <- rowDataToEdge(edgeSchema, data, types, load)
				}
				if err := rows.Err(); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetEdgeList: iterating")
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
			log.Errorln("GetVertexChannel: encountered a strange ID:", elem.ID)
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
					log.Errorln("GetVertexChannel: encountered a strange ID:", batch[i].ID)
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
				log.WithFields(log.Fields{"error": err}).Error("GetVertexChannel: Queryx")
				return
			}
			types, err := columnTypeMap(rows)
			if err != nil {
				return
			}
			defer rows.Close()
			for rows.Next() {
				data := make(map[string]interface{})
				if err := rows.MapScan(data); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetVertexChannel: MapScan")
					return
				}
				v := rowDataToVertex(g.schema.GetVertex(table), data, types, load)
				r := batchMap[v.Gid]
				for _, ri := range r {
					ri.Vertex = v
					o <- ri
				}
			}
			if err := rows.Err(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetVertexChannel: iterating")
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
			log.Errorln("GetOutChannel encountered a strange ID:", elem.ID)
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
					log.Errorln("GetOutChannel encountered a strange ID:", batch[i].ID)
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
				dropKeys := []string{}
				switch edgeSchema.Table {
				case "":
					q = fmt.Sprintf("SELECT %s.%s, %s.%s AS %s_%s FROM %s INNER JOIN %s ON %s.%s=%s.%s WHERE %s.%s IN (%s)",
						// SELECT
						edgeSchema.To.DestTable, "*",
						edgeSchema.From.DestTable, g.schema.GetVertexGid(edgeSchema.From.DestTable),
						// AS
						edgeSchema.From.DestTable, g.schema.GetVertexGid(edgeSchema.From.DestTable),
						// FROM
						edgeSchema.To.DestTable,
						// INNER JOIN
						edgeSchema.From.DestTable,
						// ON
						edgeSchema.From.DestTable, edgeSchema.From.DestField,
						edgeSchema.To.DestTable, edgeSchema.To.DestField,
						// WHERE
						edgeSchema.From.DestTable, g.schema.GetVertexGid(edgeSchema.From.DestTable),
						ids,
					)
					dataKey = fmt.Sprintf("%v_%v", edgeSchema.From.DestTable, g.schema.GetVertexGid(edgeSchema.From.DestTable))
					dropKeys = append(dropKeys, dataKey)

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
					dropKeys = append(dropKeys, edgeSchema.From.SourceField)
				}
				rows, err := g.db.Queryx(q)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetOutChannel: Queryx")
					return
				}
				types, err := columnTypeMap(rows)
				if err != nil {
					return
				}
				defer rows.Close()
				for rows.Next() {
					data := make(map[string]interface{})
					if err := rows.MapScan(data); err != nil {
						log.WithFields(log.Fields{"error": err}).Error("GetOutChannel: MapScan")
						return
					}
					r := batchMap[fmt.Sprintf("%v:%v", edgeSchema.From.DestTable, data[dataKey])]
					for _, k := range dropKeys {
						delete(data, k)
					}
					v := rowDataToVertex(g.schema.GetVertex(edgeSchema.To.DestTable), data, types, load)
					for _, ri := range r {
						ri.Vertex = v
						o <- ri
					}
				}
				if err := rows.Err(); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetOutChannel: iterating")
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
			log.Errorln("GetInChannel encountered a strange ID:", elem.ID)
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
					log.Errorln("GetInChannel encountered a strange ID:", batch[i].ID)
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
				dropKeys := []string{}
				switch edgeSchema.Table {
				case "":
					q = fmt.Sprintf("SELECT %s.%s, %s.%s AS %s_%s FROM %s INNER JOIN %s ON %s.%s=%s.%s WHERE %s.%s IN (%s)",
						// SELECT
						edgeSchema.From.DestTable, "*",
						edgeSchema.To.DestTable, g.schema.GetVertexGid(edgeSchema.To.DestTable),
						// AS
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
					dataKey = fmt.Sprintf("%v_%v", edgeSchema.To.DestTable, g.schema.GetVertexGid(edgeSchema.To.DestTable))
					dropKeys = append(dropKeys, dataKey)

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
					dropKeys = append(dropKeys, edgeSchema.To.SourceField)
				}
				rows, err := g.db.Queryx(q)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetInChannel: Queryx")
					return
				}
				types, err := columnTypeMap(rows)
				if err != nil {
					return
				}
				defer rows.Close()
				for rows.Next() {
					data := make(map[string]interface{})
					if err := rows.MapScan(data); err != nil {
						log.WithFields(log.Fields{"error": err}).Error("GetInChannel: MapScan")
						return
					}
					r := batchMap[fmt.Sprintf("%v:%v", edgeSchema.To.DestTable, data[dataKey])]
					for _, k := range dropKeys {
						delete(data, k)
					}
					v := rowDataToVertex(g.schema.GetVertex(edgeSchema.From.DestTable), data, types, load)
					for _, ri := range r {
						ri.Vertex = v
						o <- ri
					}
				}
				if err := rows.Err(); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetInChannel: iterating")
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
			log.Errorln("GetOutEdgeChannel encountered a strange ID:", elem.ID)
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
					log.Errorln("GetOutEdgeChannel encountered a strange ID:", batch[i].ID)
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
						log.WithFields(log.Fields{"error": err}).Error("GetOutEdgeChannel: Queryx")
						return
					}
					defer rows.Close()
					for rows.Next() {
						var fromGid, toGid string
						if err := rows.Scan(&fromGid, &toGid); err != nil {
							log.WithFields(log.Fields{"error": err}).Error("GetOutEdgeChannel: Scan")
							return
						}
						geid := &generatedEdgeID{edgeSchema.Label, edgeSchema.From.DestTable, fromGid, edgeSchema.To.DestTable, toGid}
						edge := geid.Edge()
						r := batchMap[edge.From]
						for _, ri := range r {
							ri.Edge = edge
							o <- ri
						}
					}
					if err := rows.Err(); err != nil {
						log.WithFields(log.Fields{"error": err}).Error("GetOutEdgeChannel: iterating")
						return
					}

				default:
					q = fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s)", edgeSchema.Table, edgeSchema.From.SourceField, ids)
					rows, err := g.db.Queryx(q)
					if err != nil {
						log.WithFields(log.Fields{"error": err}).Error("GetOutEdgeChannel: Queryx")
						return
					}
					types, err := columnTypeMap(rows)
					if err != nil {
						return
					}
					defer rows.Close()
					for rows.Next() {
						data := make(map[string]interface{})
						if err := rows.MapScan(data); err != nil {
							log.WithFields(log.Fields{"error": err}).Error("GetOutEdgeChannel: MapScan")
							return
						}
						edge := rowDataToEdge(edgeSchema, data, types, load)
						r := batchMap[fmt.Sprintf("%v:%v", edgeSchema.From.DestTable, data[edgeSchema.From.SourceField])]
						for _, ri := range r {
							ri.Edge = edge
							o <- ri
						}
					}
					if err := rows.Err(); err != nil {
						log.WithFields(log.Fields{"error": err}).Error("GetOutEdgeChannel: iterating")
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
			log.Errorln("GetInEdgeChannel encountered a strange ID:", elem.ID)
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
					log.Errorln("GetInEdgeChannel encountered a strange ID:", batch[i].ID)
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
						log.WithFields(log.Fields{"error": err}).Error("GetInEdgeChannel: Queryx")
						return
					}
					defer rows.Close()
					for rows.Next() {
						var fromGid, toGid string
						if err := rows.Scan(&fromGid, &toGid); err != nil {
							log.WithFields(log.Fields{"error": err}).Error("GetInEdgeChannel: Scan")
							return
						}
						geid := &generatedEdgeID{edgeSchema.Label, edgeSchema.From.DestTable, fromGid, edgeSchema.To.DestTable, toGid}
						edge := geid.Edge()
						r := batchMap[edge.To]
						for _, ri := range r {
							ri.Edge = edge
							o <- ri
						}
					}
					if err := rows.Err(); err != nil {
						log.WithFields(log.Fields{"error": err}).Error("GetInEdgeChannel: iterating")
						return
					}

				default:
					q = fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s)", edgeSchema.Table, edgeSchema.To.SourceField, ids)
					rows, err := g.db.Queryx(q)
					if err != nil {
						log.WithFields(log.Fields{"error": err}).Error("GetInEdgeChannel: Queryx")
						return
					}
					types, err := columnTypeMap(rows)
					if err != nil {
						return
					}
					defer rows.Close()
					for rows.Next() {
						data := make(map[string]interface{})
						if err := rows.MapScan(data); err != nil {
							log.WithFields(log.Fields{"error": err}).Error("GetInEdgeChannel: MapScan")
							return
						}
						edge := rowDataToEdge(edgeSchema, data, types, load)
						r := batchMap[fmt.Sprintf("%v:%v", edgeSchema.To.DestTable, data[edgeSchema.To.SourceField])]
						for _, ri := range r {
							ri.Edge = edge
							o <- ri
						}
					}
					if err := rows.Err(); err != nil {
						log.WithFields(log.Fields{"error": err}).Error("GetInEdgeChannel: iterating")
						return
					}
				}
			}
		}
	}()
	return o
}

// ListVertexLabels returns a list of vertex types in the graph
func (g *Graph) ListVertexLabels() ([]string, error) {
	labels := []string{}
	for _, table := range g.schema.Vertices {
		labels = append(labels, table.Label)
	}
	return labels, nil
}

// ListEdgeLabels returns a list of edge types in the graph
func (g *Graph) ListEdgeLabels() ([]string, error) {
	labels := []string{}
	for _, table := range g.schema.Edges {
		labels = append(labels, table.Label)
	}
	return labels, nil
}
