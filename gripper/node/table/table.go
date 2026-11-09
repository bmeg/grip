// Package table maps flat tables (CSV or JSON lines files) onto the vertices and
// edges of a resource node's graph. It replaces the table-to-graph mapping that the
// GRIPPER v1 Python proxies did centrally.
package table

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/bmeg/grip/gdbi"
	"sigs.k8s.io/yaml"
)

// Mapping describes how tables become a graph. All generated vertex and edge IDs must
// start with the node prefix passed to Load.
//
//	vertices:
//	  - file: characters.csv
//	    label: Character
//	    prefix: "Character:"   # added to the value of the id field
//	    id: id
//	edges:
//	  - file: characters.csv
//	    label: homeworld
//	    from: {field: id, prefix: "Character:"}
//	    to:   {field: homeworld, prefix: "Planet:"}
//
// An edge row is any row of the file with a non-empty value in both fields, so one
// file can hold vertices and edges (a field pointing at another table) or be a
// dedicated edge table.
type Mapping struct {
	Vertices []VertexTable `json:"vertices"`
	Edges    []EdgeTable   `json:"edges"`
}

type VertexTable struct {
	File   string `json:"file"`
	Label  string `json:"label"`
	Prefix string `json:"prefix"`
	ID     string `json:"id"`
}

type EdgeTable struct {
	File  string   `json:"file"`
	Label string   `json:"label"`
	From  Endpoint `json:"from"`
	To    Endpoint `json:"to"`
}

type Endpoint struct {
	Field  string `json:"field"`
	Prefix string `json:"prefix"`
}

// LoadMapping reads a YAML mapping file.
func LoadMapping(path string) (*Mapping, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	m := &Mapping{}
	if err := yaml.Unmarshal(b, m); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	return m, nil
}

const batchSize = 1000

// Load reads the tables of the mapping (relative file names are resolved against
// baseDir) and adds the resulting vertices and edges to g. It returns the number of
// vertices and edges added.
func Load(ctx context.Context, g gdbi.GraphInterface, m *Mapping, baseDir, nodePrefix string) (int, int, error) {
	nv, ne := 0, 0
	vbatch := []*gdbi.Vertex{}
	flushV := func() error {
		if len(vbatch) == 0 {
			return nil
		}
		err := g.AddVertex(vbatch)
		nv += len(vbatch)
		vbatch = vbatch[:0]
		return err
	}
	for _, vt := range m.Vertices {
		err := readRows(filepath.Join(baseDir, vt.File), func(row map[string]interface{}) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			key := text(row[vt.ID])
			if key == "" {
				return nil
			}
			id := vt.Prefix + key
			if !strings.HasPrefix(id, nodePrefix) {
				return fmt.Errorf("%s: vertex %q does not start with the node prefix %q", vt.File, id, nodePrefix)
			}
			vbatch = append(vbatch, &gdbi.Vertex{ID: id, Label: vt.Label, Data: row, Loaded: true})
			if len(vbatch) >= batchSize {
				return flushV()
			}
			return nil
		})
		if err != nil {
			return nv, ne, err
		}
	}
	if err := flushV(); err != nil {
		return nv, ne, err
	}

	ebatch := []*gdbi.Edge{}
	flushE := func() error {
		if len(ebatch) == 0 {
			return nil
		}
		err := g.AddEdge(ebatch)
		ne += len(ebatch)
		ebatch = ebatch[:0]
		return err
	}
	for _, et := range m.Edges {
		err := readRows(filepath.Join(baseDir, et.File), func(row map[string]interface{}) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			from, to := text(row[et.From.Field]), text(row[et.To.Field])
			if from == "" || to == "" {
				return nil
			}
			fromID, toID := et.From.Prefix+from, et.To.Prefix+to
			id := fromID + "-" + et.Label + "-" + toID
			if !strings.HasPrefix(id, nodePrefix) {
				return fmt.Errorf("%s: edge %q does not start with the node prefix %q; edges are stored with their source vertex", et.File, id, nodePrefix)
			}
			ebatch = append(ebatch, &gdbi.Edge{ID: id, From: fromID, To: toID, Label: et.Label, Data: map[string]interface{}{}, Loaded: true})
			if len(ebatch) >= batchSize {
				return flushE()
			}
			return nil
		})
		if err != nil {
			return nv, ne, err
		}
	}
	return nv, ne, flushE()
}

func text(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case json.Number:
		return x.String()
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	}
	return fmt.Sprint(v)
}

// readRows calls fn with every row of a .csv (header row required) or JSON lines file.
func readRows(path string, fn func(map[string]interface{}) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if strings.HasSuffix(path, ".csv") {
		return readCSV(f, fn)
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<28)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		row := map[string]interface{}{}
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if err := fn(row); err != nil {
			return err
		}
	}
	return sc.Err()
}

func readCSV(r io.Reader, fn func(map[string]interface{}) error) error {
	cr := csv.NewReader(r)
	header, err := cr.Read()
	if err != nil {
		return err
	}
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		row := make(map[string]interface{}, len(header))
		for i, h := range header {
			if i < len(rec) && rec[i] != "" {
				row[h] = typed(rec[i])
			}
		}
		if err := fn(row); err != nil {
			return err
		}
	}
}

// typed converts CSV cells that look like numbers or booleans.
func typed(s string) interface{} {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	switch s {
	case "true":
		return true
	case "false":
		return false
	}
	return s
}
