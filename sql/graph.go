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
	return RowDataToVertex(table, g.schema, data, load)
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
	return RowDataToEdge(table, g.schema, data, load)
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
			rows, err := g.db.Queryx(q)
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
				o <- RowDataToVertex(v.Table, g.schema, data, load)
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
				rows, err := g.db.Queryx(q)
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
					v := RowDataToVertex(v.Table, g.schema, data, false)
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
		for _, v := range g.schema.Edges {
			q := ""
			switch v.Table {
			case "":
				q = fmt.Sprintf("SELECT %s.%s, %s.%s FROM %s INNER JOIN %s ON %s.%s=%s.%s",
					// SELECT
					v.From.DestTable, v.From.DestGid,
					v.To.DestTable, v.To.DestGid,
					// FROM
					v.From.DestTable,
					// INNER JOIN
					v.To.DestTable,
					// ON
					v.From.DestTable, v.From.DestField,
					v.To.DestTable, v.To.DestField)
				rows, err := g.db.Queryx(q)
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
					geid := &generatedEdgeID{v.Label, v.From.DestTable, fromGid, v.To.DestTable, toGid}
					// TODO figure out how to get label
					o <- &aql.Edge{
						Gid:   geid.String(),
						Label: v.Label,
						From:  fromGid,
						To:    toGid,
						Data:  nil,
					}
				}
				if err := rows.Err(); err != nil {
					log.Println("GetEdgeList failed:", err)
					return
				}

			default:
				q = fmt.Sprintf("SELECT * FROM %s", v.Table)
				rows, err := g.db.Queryx(q)
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
					o <- RowDataToEdge(v.Table, g.schema, data, load)
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
func (g *Graph) GetVertexChannel(ids chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
	}()
	return o
}

// GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (g *Graph) GetOutChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
	}()
	return o
}

// GetInChannel process requests of vertex ids and find the connected vertices on incoming edges
func (g *Graph) GetInChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
	}()
	return o
}

// GetOutEdgeChannel process requests of vertex ids and find the connected outgoing edges
func (g *Graph) GetOutEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
	}()
	return o
}

// GetInEdgeChannel process requests of vertex ids and find the connected incoming edges
func (g *Graph) GetInEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
	}()
	return o
}
