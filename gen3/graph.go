package gen3

import (
	"context"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

const batchSize int = 1000

// Graph is the interface to a single graph
type Graph struct {
	db     *sqlx.DB
	v      string
	e      string
	graph  string
	psql   sq.StatementBuilderType
	layout *graphConfig
}

// Compiler returns a query compiler that uses the graph
func (g *Graph) Compiler() gdbi.Compiler {
	return core.NewCompiler(g)
}

////////////////////////////////////////////////////////////////////////////////
// Write methods
////////////////////////////////////////////////////////////////////////////////

// AddVertex adds a vertex to the database
func (g *Graph) AddVertex(vertices []*gripql.Vertex) error {
	return fmt.Errorf("not implemented")
}

// AddEdge adds an edge to the database
func (g *Graph) AddEdge(edges []*gripql.Edge) error {
	return fmt.Errorf("not implemented")
}

// DelVertex is not implemented in the SQL driver
func (g *Graph) DelVertex(key string) error {
	return fmt.Errorf("not implemented")
}

// DelEdge is not implemented in the SQL driver
func (g *Graph) DelEdge(key string) error {
	return fmt.Errorf("not implemented")
}

////////////////////////////////////////////////////////////////////////////////
// Read methods
////////////////////////////////////////////////////////////////////////////////

// GetTimestamp gets the timestamp of last update
func (g *Graph) GetTimestamp() string {
	return "not implemented"
}

// GetVertex loads a vertex given an id. It returns a nil if not found.
func (g *Graph) GetVertex(gid string, load bool) *gripql.Vertex {
	// g.psql.Select("node_id").From().Where(sq.Eq{"gid": gid})
	q := fmt.Sprintf(`SELECT gid, label FROM %s WHERE gid='%s'`, g.v, gid)
	if load {
		q = fmt.Sprintf(`SELECT * FROM %s WHERE gid='%s'`, g.v, gid)
	}
	vrow := &row{}
	err := g.db.QueryRowx(q).StructScan(vrow)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "query": q}).Error("GetVertex: StructScan")
		return nil
	}
	vertex, err := convertVertexRow(vrow, load)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("GetVertex: convertVertexRow")
		return nil
	}
	return vertex
}

// GetEdge loads an edge  given an id. It returns a nil if not found.
func (g *Graph) GetEdge(gid string, load bool) *gripql.Edge {
	q := fmt.Sprintf(`SELECT gid, label, "from", "to" FROM %s WHERE gid='%s'`, g.e, gid)
	if load {
		q = fmt.Sprintf(`SELECT * FROM %s WHERE gid='%s'`, g.e, gid)
	}
	erow := &row{}
	err := g.db.QueryRowx(q).StructScan(erow)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "query": q}).Error("GetEdge: StructScan")
		return nil
	}
	edge, err := convertEdgeRow(erow, load)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("GetEdge: convertEdgeRow")
		return nil
	}
	return edge
}

// GetVertexList produces a channel of all vertices in the graph
func (g *Graph) GetVertexList(ctx context.Context, load bool) <-chan *gripql.Vertex {
	o := make(chan *gripql.Vertex, 100)
	go func() {
		defer close(o)
		q := fmt.Sprintf("SELECT gid, label FROM %s", g.v)
		if load {
			q = fmt.Sprintf(`SELECT * FROM %s`, g.v)
		}
		rows, err := g.db.QueryxContext(ctx, q)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetVertexList: QueryxContext")
			return
		}
		defer rows.Close()
		for rows.Next() {
			vrow := &row{}
			if err := rows.StructScan(vrow); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetVertexList: StructScan")
				continue
			}
			v, err := convertVertexRow(vrow, load)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetVertexList: convertVertexRow")
				continue
			}
			o <- v
		}
		if err := rows.Err(); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetVertexList: iterating")
		}
	}()
	return o
}

// VertexLabelScan produces a channel of all vertex ids where the vertex label matches `label`
func (g *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	o := make(chan string, 100)
	go func() {
		defer close(o)
		q := fmt.Sprintf("SELECT gid FROM %s WHERE label='%s'", g.v, label)
		rows, err := g.db.QueryxContext(ctx, q)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("VertexLabelScan: QueryxContext")
			return
		}
		defer rows.Close()
		for rows.Next() {
			var gid string
			if err := rows.Scan(&gid); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("VertexLabelScan: Scan")
				continue
			}
			o <- gid
		}
		if err := rows.Err(); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("VertexLabelScan: iterating")
		}
	}()
	return o
}

// GetEdgeList produces a channel of all edges in the graph
func (g *Graph) GetEdgeList(ctx context.Context, load bool) <-chan *gripql.Edge {
	o := make(chan *gripql.Edge, 100)
	go func() {
		defer close(o)
		q := fmt.Sprintf(`SELECT gid, label, "from", "to" FROM %s`, g.e)
		if load {
			q = fmt.Sprintf(`SELECT * FROM %s`, g.e)
		}
		rows, err := g.db.QueryxContext(ctx, q)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetEdgeList: QueryxContext")
			return
		}
		defer rows.Close()
		for rows.Next() {
			erow := &row{}
			if err := rows.StructScan(erow); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetEdgeList: StructScan")
				continue
			}
			e, err := convertEdgeRow(erow, load)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetEdgeList: convertEdgeRow")
				continue
			}
			o <- e
		}
		if err := rows.Err(); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("GetEdgeList: iterating")
		}
	}()
	return o
}

// GetVertexChannel is passed a channel of vertex ids and it produces a channel of vertices
func (g *Graph) GetVertexChannel(reqChan chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, batchSize)
		for id := range reqChan {
			o = append(o, id)
			if len(o) >= batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, batchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for batch := range batches {
			idBatch := make([]string, len(batch))
			for i := range batch {
				idBatch[i] = fmt.Sprintf("'%s'", batch[i].ID)
			}
			ids := strings.Join(idBatch, ", ")
			q := fmt.Sprintf("SELECT gid, label FROM %s WHERE gid IN (%s)", g.v, ids)
			if load {
				q = fmt.Sprintf("SELECT * FROM %s WHERE gid IN (%s)", g.v, ids)
			}
			rows, err := g.db.Queryx(q)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetVertexChannel: Queryx")
				return
			}
			defer rows.Close()
			chunk := map[string]*gripql.Vertex{}
			for rows.Next() {
				vrow := &row{}
				if err := rows.StructScan(vrow); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetVertexChannel: StructScan")
					continue
				}
				v, err := convertVertexRow(vrow, load)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetVertexChannel: convertVertexRow")
					continue
				}
				chunk[v.Gid] = v
			}
			if err := rows.Err(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetVertexChannel: iterating")
			}
			for _, id := range batch {
				if x, ok := chunk[id.ID]; ok {
					id.Vertex = x
					o <- id
				}
			}
		}
	}()
	return o
}

// GetOutChannel is passed a channel of vertex ids and finds the connected vertices via outgoing edges
func (g *Graph) GetOutChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, batchSize)
		for id := range reqChan {
			o = append(o, id)
			if len(o) >= batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, batchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = fmt.Sprintf("'%s'", batch[i].ID)
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			ids := strings.Join(idBatch, ", ")
			q := fmt.Sprintf(
				"SELECT %s.gid, %s.label, %s.from FROM %s INNER JOIN %s ON %s.to=%s.gid WHERE %s.from IN (%s)",
				// SELECT
				g.v, g.v, g.e,
				// FROM
				g.v,
				// INNER JOIN
				g.e,
				// ON
				g.e, g.v,
				// WHERE
				g.e,
				// IN
				ids,
			)
			if load {
				q = fmt.Sprintf(
					"SELECT %s.*, %s.from FROM %s INNER JOIN %s ON %s.to=%s.gid WHERE %s.from IN (%s)",
					// SELECT
					g.v, g.e,
					// FROM
					g.v,
					// INNER JOIN
					g.e,
					// ON
					g.e, g.v,
					// WHERE
					g.e,
					// IN
					ids,
				)
			}
			if len(edgeLabels) > 0 {
				labels := make([]string, len(edgeLabels))
				for i := range edgeLabels {
					labels[i] = fmt.Sprintf("'%s'", edgeLabels[i])
				}
				q = fmt.Sprintf("%s AND %s.label IN (%s)", q, g.e, strings.Join(labels, ", "))
			}
			rows, err := g.db.Queryx(q)
			if err != nil {
				log.WithFields(log.Fields{"error": err, "query": q}).Error("GetOutChannel: Queryx")
				return
			}
			defer rows.Close()
			for rows.Next() {
				vrow := &row{}
				if err := rows.StructScan(vrow); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetOutChannel: StructScan")
					continue
				}
				v, err := convertVertexRow(vrow, load)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetOutChannel: convertVertexRow")
					continue
				}
				r := batchMap[vrow.From]
				for _, ri := range r {
					ri.Vertex = v
					o <- ri
				}
			}
			if err := rows.Err(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetOutChannel: iterating")
			}
		}
	}()
	return o
}

// GetInChannel is passed a channel of vertex ids and finds the connected vertices via incoming edges
func (g *Graph) GetInChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, batchSize)
		for id := range reqChan {
			o = append(o, id)
			if len(o) >= batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, batchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = fmt.Sprintf("'%s'", batch[i].ID)
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			ids := strings.Join(idBatch, ", ")
			q := fmt.Sprintf(
				"SELECT %s.gid, %s.label, %s.to FROM %s INNER JOIN %s ON %s.from=%s.gid WHERE %s.to IN (%s)",
				// SELECT
				g.v, g.v, g.e,
				// FROM
				g.v,
				// INNER JOIN
				g.e,
				// ON
				g.e, g.v,
				// WHERE
				g.e,
				// IN
				ids,
			)
			if load {
				q = fmt.Sprintf(
					"SELECT %s.*, %s.to FROM %s INNER JOIN %s ON %s.from=%s.gid WHERE %s.to IN (%s)",
					// SELECT
					g.v, g.e,
					// FROM
					g.v,
					// INNER JOIN
					g.e,
					// ON
					g.e, g.v,
					// WHERE
					g.e,
					// IN
					ids,
				)
			}
			if len(edgeLabels) > 0 {
				labels := make([]string, len(edgeLabels))
				for i := range edgeLabels {
					labels[i] = fmt.Sprintf("'%s'", edgeLabels[i])
				}
				q = fmt.Sprintf("%s AND %s.label IN (%s)", q, g.e, strings.Join(labels, ", "))
			}
			rows, err := g.db.Queryx(q)
			if err != nil {
				log.WithFields(log.Fields{"error": err, "query": q}).Error("GetInChannel: Queryx")
				return
			}
			defer rows.Close()
			for rows.Next() {
				vrow := &row{}
				if err := rows.StructScan(vrow); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetInChannel: StructScan")
					continue
				}
				v, err := convertVertexRow(vrow, load)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetInChannel: convertVertexRow")
					continue
				}
				r := batchMap[vrow.To]
				for _, ri := range r {
					ri.Vertex = v
					o <- ri
				}
			}
			if err := rows.Err(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetInChannel: iterating")
			}
		}
	}()
	return o
}

// GetOutEdgeChannel is passed a channel of vertex ids and finds the outgoing edges
func (g *Graph) GetOutEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, batchSize)
		for id := range reqChan {
			o = append(o, id)
			if len(o) >= batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, batchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = fmt.Sprintf("'%s'", batch[i].ID)
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			ids := strings.Join(idBatch, ", ")
			q := fmt.Sprintf(
				`SELECT gid, label, "from", "to" FROM %s WHERE %s.from IN (%s)`,
				// FROM
				g.e,
				// WHERE
				g.e,
				// IN
				ids,
			)
			if load {
				q = fmt.Sprintf(
					"SELECT * FROM %s WHERE %s.from IN (%s)",
					// FROM
					g.e,
					// WHERE
					g.e,
					// IN
					ids,
				)
			}
			if len(edgeLabels) > 0 {
				labels := make([]string, len(edgeLabels))
				for i := range edgeLabels {
					labels[i] = fmt.Sprintf("'%s'", edgeLabels[i])
				}
				q = fmt.Sprintf("%s AND %s.label IN (%s)", q, g.e, strings.Join(labels, ", "))
			}
			rows, err := g.db.Queryx(q)
			if err != nil {
				log.WithFields(log.Fields{"error": err, "query": q}).Error("GetOutEdgeChannel: Queryx")
				return
			}
			defer rows.Close()
			for rows.Next() {
				erow := &row{}
				if err := rows.StructScan(erow); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetOutEdgeChannel: StructScan")
					continue
				}
				e, err := convertEdgeRow(erow, load)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetOutEdgeChannel: convertEdgeRow")
					continue
				}
				r := batchMap[erow.From]
				for _, ri := range r {
					ri.Edge = e
					o <- ri
				}
			}
			if err := rows.Err(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetOutEdgeChannel: iterating")
			}
		}
	}()
	return o
}

// GetInEdgeChannel is passed a channel of vertex ids and finds the incoming edges
func (g *Graph) GetInEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, batchSize)
		for id := range reqChan {
			o = append(o, id)
			if len(o) >= batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, batchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = fmt.Sprintf("'%s'", batch[i].ID)
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			ids := strings.Join(idBatch, ", ")
			q := fmt.Sprintf(
				`SELECT gid, label, "from", "to" FROM %s WHERE %s.to IN (%s)`,
				// FROM
				g.e,
				// WHERE
				g.e,
				// IN
				ids,
			)
			if load {
				q = fmt.Sprintf(
					"SELECT * FROM %s WHERE %s.to IN (%s)",
					// FROM
					g.e,
					// WHERE
					g.e,
					// IN
					ids,
				)
			}
			if len(edgeLabels) > 0 {
				labels := make([]string, len(edgeLabels))
				for i := range edgeLabels {
					labels[i] = fmt.Sprintf("'%s'", edgeLabels[i])
				}
				q = fmt.Sprintf("%s AND %s.label IN (%s)", q, g.e, strings.Join(labels, ", "))
			}
			rows, err := g.db.Queryx(q)
			if err != nil {
				log.WithFields(log.Fields{"error": err, "query": q}).Error("GetInEdgeChannel: Queryx")
				return
			}
			defer rows.Close()
			for rows.Next() {
				erow := &row{}
				if err := rows.StructScan(erow); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetInEdgeChannel: StructScan")
					continue
				}
				e, err := convertEdgeRow(erow, load)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetInEdgeChannel: convertEdgeRow")
					continue
				}
				r := batchMap[erow.To]
				for _, ri := range r {
					ri.Edge = e
					o <- ri
				}
			}
			if err := rows.Err(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetInEdgeChannel: iterating")
			}
		}
	}()
	return o
}

// ListVertexLabels returns a list of vertex types in the graph
func (g *Graph) ListVertexLabels() ([]string, error) {
	labels := []string{}
	for l := range g.layout.vertices {
		labels = append(labels, l)
	}
	return labels, nil
}

// ListEdgeLabels returns a list of edge types in the graph
func (g *Graph) ListEdgeLabels() ([]string, error) {
	labels := []string{}
	for l := range g.layout.edges {
		labels = append(labels, l)
	}
	return labels, nil
}
