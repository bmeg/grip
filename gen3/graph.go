package gen3

import (
	"context"
	"fmt"

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

func (g *Graph) getVertex(gid string, table string, load bool) (*gripql.Vertex, error) {
	q, args, err := g.psql.Select("node_id", "_props").
		From(table).
		Where(sq.Eq{"node_id": gid}).
		ToSql()
	if err != nil {
		return nil, err
	}
	vrow := &row{}
	err = g.db.QueryRowx(q, args...).StructScan(vrow)
	if err != nil {
		return nil, err
	}
	return convertVertexRow(vrow, g.layout.label(table), load)
}

// GetVertex loads a vertex given an id. It returns a nil if not found.
func (g *Graph) GetVertex(gid string, load bool) *gripql.Vertex {
	var v *gripql.Vertex
	for _, table := range g.layout.listVertexTables() {
		v, err := g.getVertex(gid, table, load)
		if err != nil {
			if noRowsInResult(err) || tableDoesNotExist(err) {
				continue
			}
			log.WithFields(log.Fields{"error": err}).Error("GetVertex")
			return nil
		}
		return v
	}
	return v
}

func (g *Graph) getEdge(srcID, dstID, table string, load bool) (*gripql.Edge, error) {
	q, args, err := g.psql.Select("src_id", "dst_id", "_props").
		From(table).
		Where(sq.Eq{"src_id": srcID, "dst_id": dstID}).
		ToSql()
	if err != nil {
		return nil, err
	}
	erow := &row{}
	err = g.db.QueryRowx(q, args...).StructScan(erow)
	if err != nil {
		return nil, err
	}
	return convertEdgeRow(erow, g.layout.label(table), load)
}

// GetEdge loads an edge  given an id. It returns a nil if not found.
func (g *Graph) GetEdge(gid string, load bool) *gripql.Edge {
	var e *gripql.Edge
	srcID, dstID := getEdgeIDParts(gid)
	for _, table := range g.layout.listEdgeTables() {
		e, err := g.getEdge(srcID, dstID, table, load)
		if err != nil {
			if noRowsInResult(err) || tableDoesNotExist(err) {
				continue
			}
			log.WithFields(log.Fields{"error": err}).Error("GetEdge")
			return nil
		}
		return e
	}
	return e
}

// GetVertexList produces a channel of all vertices in the graph
func (g *Graph) GetVertexList(ctx context.Context, load bool) <-chan *gripql.Vertex {
	o := make(chan *gripql.Vertex, 100)
	go func() {
		defer close(o)
		for _, table := range g.layout.listVertexTables() {
			q, args, err := g.psql.Select("node_id", "_props").From(table).ToSql()
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetVertexList: ToSql")
				return
			}
			rows, err := g.db.QueryxContext(ctx, q, args...)
			if err != nil {
				if noRowsInResult(err) || tableDoesNotExist(err) {
					continue
				}
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
				v, err := convertVertexRow(vrow, g.layout.label(table), load)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetVertexList: convertVertexRow")
					continue
				}
				o <- v
			}
			if err := rows.Err(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetVertexList: iterating")
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
		table := g.layout.table(label)
		if table == "" {
			log.Errorf("VertexLabelScan: unknown label '%s'", label)
			return
		}
		q, args, err := g.psql.Select("node_id").
			From(table).
			ToSql()
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error("VertexLabelScan: ToSql")
			return
		}
		rows, err := g.db.QueryxContext(ctx, q, args...)
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
		for _, table := range g.layout.listEdgeTables() {
			q, args, err := g.psql.Select("src_id", "dst_id", "_props").From(table).ToSql()
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetVertexList: ToSql")
				return
			}
			rows, err := g.db.QueryxContext(ctx, q, args...)
			if err != nil {
				if noRowsInResult(err) || tableDoesNotExist(err) {
					continue
				}
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
				e, err := convertEdgeRow(erow, g.layout.label(table), load)
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetEdgeList: convertEdgeRow")
					continue
				}
				o <- e
			}
			if err := rows.Err(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetEdgeList: iterating")
			}
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
		for req := range reqChan {
			o = append(o, req)
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
				idBatch[i] = batch[i].ID
			}
			for _, table := range g.layout.listVertexTables() {
				q, args, err := g.psql.Select("node_id", "_props").
					From(table).
					Where(sq.Eq{"node_id": idBatch}).
					ToSql()
				if err != nil {
					log.WithFields(log.Fields{"error": err}).Error("GetVertexChannel: ToSql")
					return
				}
				rows, err := g.db.Queryx(q, args...)
				if err != nil {
					if noRowsInResult(err) || tableDoesNotExist(err) {
						continue
					}
					log.WithFields(log.Fields{"error": err, "query": q}).Error("GetVertexChannel: Queryx")
					return
				}
				defer rows.Close()
				chunk := map[string]*gripql.Vertex{}
				for rows.Next() {
					vrow := &row{}
					if err := rows.StructScan(vrow); err != nil {
						log.WithFields(log.Fields{"error": err, "query": q}).Error("GetVertexChannel: StructScan")
						continue
					}
					v, err := convertVertexRow(vrow, g.layout.label(table), load)
					if err != nil {
						log.WithFields(log.Fields{"error": err, "query": q}).Error("GetVertexChannel: convertVertexRow")
						continue
					}
					chunk[v.Gid] = v
				}
				if err := rows.Err(); err != nil {
					log.WithFields(log.Fields{"error": err, "query": q}).Error("GetVertexChannel: iterating")
				}
				for _, id := range batch {
					if x, ok := chunk[id.ID]; ok {
						id.Vertex = x
						o <- id
					}
				}
			}
		}
	}()
	return o
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (g *Graph) lookupLinkedVertices(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string, direction string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	reqChanMap := make(map[string][]gdbi.ElementLookup)
	go func() {
		defer close(batches)
		// group lookups by label
		for req := range reqChan {
			label := req.GetRefVertex().Label
			reqChanMap[label] = append(reqChanMap[label], req)
		}
		for _, reqs := range reqChanMap {
			if len(reqs) <= batchSize {
				batches <- reqs
			} else {
				var i, j int
				total := len(reqs)
				for i <= total {
					j = min(i+batchSize, total)
					batches <- reqs[i:j]
					i += batchSize
				}
			}
		}
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			label := batch[0].GetRefVertex().Label
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}

			var getEdgeDefs func(string) map[string][]*edgeDef
			switch direction {
			case "out":
				getEdgeDefs = g.layout.out
			case "in":
				getEdgeDefs = g.layout.in
			default:
				log.Error("lookupLinkedVertices: invalid direction argument")
				return
			}

			edgeDefs := []*edgeDef{}
			if len(edgeLabels) > 0 {
				for elabel, e := range getEdgeDefs(label) {
					for _, l := range edgeLabels {
						if elabel == l {
							edgeDefs = append(edgeDefs, e...)
						}
					}
				}
			} else {
				for _, e := range getEdgeDefs(label) {
					edgeDefs = append(edgeDefs, e...)
				}
			}

			for _, e := range edgeDefs {
				var esrc, edst, vLabel string
				switch direction {
				case "out":
					esrc = "src_id"
					edst = "dst_id"
					vLabel = e.dstLabel
					if e.backref {
						vLabel = e.srcLabel
						esrc = "dst_id"
						edst = "src_id"
					}
				case "in":
					esrc = "dst_id"
					edst = "src_id"
					vLabel = e.srcLabel
					if e.backref {
						vLabel = e.dstLabel
						esrc = "src_id"
						edst = "dst_id"
					}
				default:
					log.Error("lookupLinkedVertices: invalid direction argument")
					return
				}
				dstTable := g.layout.table(vLabel)
				if dstTable == "" {
					log.Errorf("lookupLinkedVertices: unknown destination vertex label '%s'", vLabel)
					return
				}
				q, args, err := g.psql.
					Select(fmt.Sprintf("%s.node_id, %s._props, %s.%s AS src_id", dstTable, dstTable, e.table, esrc)).
					From(e.table).
					JoinClause(fmt.Sprintf("INNER JOIN %s ON %s.node_id = %s.%s", dstTable, dstTable, e.table, edst)).
					Where(sq.Eq{fmt.Sprintf("%s.%s", e.table, esrc): idBatch}).
					ToSql()

				rows, err := g.db.Queryx(q, args...)
				if err != nil {
					log.WithFields(log.Fields{"error": err, "query": q}).Error("lookupLinkedVertices: Queryx")
					return
				}
				defer rows.Close()
				for rows.Next() {
					vrow := &row{}
					if err := rows.StructScan(vrow); err != nil {
						log.WithFields(log.Fields{"error": err}).Error("lookupLinkedVertices: StructScan")
						continue
					}
					v, err := convertVertexRow(vrow, vLabel, load)
					if err != nil {
						log.WithFields(log.Fields{"error": err}).Error("lookupLinkedVertices: convertVertexRow")
						continue
					}

					r := batchMap[vrow.SrcID]
					for _, ri := range r {
						ri.Vertex = v
						o <- ri
					}
				}
				if err := rows.Err(); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("lookupLinkedVertices: iterating")
				}
			}
		}
	}()
	return o
}

// GetOutChannel is passed a channel of vertex ids and finds the connected vertices via outgoing edges
func (g *Graph) GetOutChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	return g.lookupLinkedVertices(reqChan, load, edgeLabels, "out")
}

// GetInChannel is passed a channel of vertex ids and finds the connected vertices via incoming edges
func (g *Graph) GetInChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	return g.lookupLinkedVertices(reqChan, load, edgeLabels, "in")
}

func (g *Graph) lookupLinkedEdges(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string, direction string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	reqChanMap := make(map[string][]gdbi.ElementLookup)
	go func() {
		defer close(batches)
		// group lookups by label
		for req := range reqChan {
			label := req.GetRefVertex().Label
			reqChanMap[label] = append(reqChanMap[label], req)
		}
		for _, reqs := range reqChanMap {
			if len(reqs) <= batchSize {
				batches <- reqs
			} else {
				var i, j int
				total := len(reqs)
				for i <= total {
					j = min(i+batchSize, total)
					batches <- reqs[i:j]
					i += batchSize
				}
			}
		}
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			label := batch[0].GetRefVertex().Label
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}

			var getEdgeDefs func(string) map[string][]*edgeDef
			switch direction {
			case "out":
				getEdgeDefs = g.layout.out
			case "in":
				getEdgeDefs = g.layout.in
			default:
				log.Error("lookupLinkedEdges: invalid direction argument")
				return
			}

			edgeDefs := []*edgeDef{}
			if len(edgeLabels) > 0 {
				for elabel, e := range getEdgeDefs(label) {
					for _, l := range edgeLabels {
						if elabel == l {
							edgeDefs = append(edgeDefs, e...)
						}
					}
				}
			} else {
				for _, e := range getEdgeDefs(label) {
					edgeDefs = append(edgeDefs, e...)
				}
			}

			for _, e := range edgeDefs {
				var esrc string
				switch direction {
				case "out":
					esrc = "src_id"
					if e.backref {
						esrc = "dst_id"
					}
				case "in":
					esrc = "dst_id"
					if e.backref {
						esrc = "src_id"
					}
				default:
					log.Error("lookupLinkedEdges: invalid direction argument")
					return
				}

				q, args, err := g.psql.
					Select("src_id", "dst_id", "_props").
					From(e.table).
					Where(sq.Eq{esrc: idBatch}).
					ToSql()

				rows, err := g.db.Queryx(q, args...)
				if err != nil {
					log.WithFields(log.Fields{"error": err, "query": q}).Error("lookupLinkedEdges: Queryx")
					return
				}
				defer rows.Close()
				for rows.Next() {
					erow := &row{}
					if err := rows.StructScan(erow); err != nil {
						log.WithFields(log.Fields{"error": err}).Error("lookupLinkedEdges: StructScan")
						continue
					}
					e, err := convertEdgeRow(erow, e.label, load)
					if err != nil {
						log.WithFields(log.Fields{"error": err}).Error("lookupLinkedEdges: convertEdgeRow")
						continue
					}

					var r []gdbi.ElementLookup
					if esrc == "src_id" {
						r = batchMap[erow.SrcID]
					} else if esrc == "dst_id" {
						r = batchMap[erow.DstID]
					}
					for _, ri := range r {
						ri.Edge = e
						o <- ri
					}
				}
				if err := rows.Err(); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("lookupLinkedEdges: iterating")
				}
			}
		}
	}()
	return o
}

// GetOutEdgeChannel is passed a channel of vertex ids and finds the outgoing edges
func (g *Graph) GetOutEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	return g.lookupLinkedEdges(reqChan, load, edgeLabels, "out")
}

// GetInEdgeChannel is passed a channel of vertex ids and finds the incoming edges
func (g *Graph) GetInEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	return g.lookupLinkedEdges(reqChan, load, edgeLabels, "in")
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
