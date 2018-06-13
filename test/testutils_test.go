package test

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/config"
	"github.com/bmeg/arachne/protoutil"
	"github.com/bmeg/arachne/sql"
	"github.com/bmeg/arachne/util"
)

var vertices = []*aql.Vertex{
	vertex("Person", data{"name": "Obi-Wan Kenobi", "height": 182, "occupation": "jedi", "species": "human"}),    // 0
	vertex("Person", data{"name": "Luke Skywalker", "height": 172, "occupation": "jedi", "species": "human"}),    // 1
	vertex("Person", data{"name": "Han Solo", "height": 180, "occupation": "smuggler", "species": "human"}),      // 2
	vertex("Person", data{"name": "Leia Organa", "height": 150, "occupation": "politician", "species": "human"}), // 3
	vertex("Person", data{"name": "Darth Vader", "height": 202, "occupation": "sith", "species": "human"}),       // 4
	vertex("Person", data{"name": "Chewbacca", "height": 228, "occupation": "smuggler", "species": "wookie"}),    // 5
	vertex("Person", data{"name": "Yoda", "height": 66, "occupation": "jedi", "species": nil}),                   // 6

	vertex("Droid", data{"name": "C-3PO", "height": 167}), // 7
	vertex("Droid", data{"name": "R2-D2", "height": 96}),  // 8

	vertex("Starship", data{"name": "Millennium Falcon", "model": "YT-1300 light freighter"}), // 9
	vertex("Starship", data{"name": "Death Star", "model": "DS-1 Orbital Battle Station"}),    // 10
	vertex("Starship", data{"name": "X-wing", "model": "T-65 X-wing"}),                        // 11
	vertex("Starship", data{"name": "TIE Fighter", "model": "Twin Ion Engine Fighter"}),       // 12

	vertex("Planet", data{"name": "Tatooine", "diameter": 10465, "population": 200000}),     // 13
	vertex("Planet", data{"name": "Alderan", "diameter": 12500, "population": 2000000000}),  // 14
	vertex("Planet", data{"name": "Dagobah", "diameter": 8900, "population": nil}),          // 15
	vertex("Planet", data{"name": "Kashyyyk", "diameter": 12765, "population": 45000000}),   // 16
	vertex("Planet", data{"name": "Corellia", "diameter": 11000, "population": 3000000000}), // 17

	vertex("Film", data{"title": "A New Hope", "episode": "IV"}),         // 18
	vertex("Film", data{"title": "Empire Strikes Back", "episode": "V"}), // 19
	vertex("Film", data{"title": "Return of the Jedi", "episode": "VI"}), // 20
}

var edges = []*aql.Edge{
	edge(vertices[0], vertices[13], "LivedOn", data{"homeworld": false}), // 0
	edge(vertices[1], vertices[13], "LivedOn", data{"homeworld": true}),  // 1
	edge(vertices[2], vertices[17], "LivedOn", data{"homeworld": true}),  // 2
	edge(vertices[3], vertices[14], "LivedOn", data{"homeworld": true}),  // 3
	edge(vertices[4], vertices[13], "LivedOn", data{"homeworld": true}),  // 4
	edge(vertices[5], vertices[16], "LivedOn", data{"homeworld": true}),  // 5
	edge(vertices[6], vertices[15], "LivedOn", data{"homeworld": false}), // 6

	edge(vertices[1], vertices[11], "Piloted", nil), // 7
	edge(vertices[2], vertices[9], "Piloted", nil),  // 8
	edge(vertices[4], vertices[12], "Piloted", nil), // 9
	edge(vertices[5], vertices[9], "Piloted", nil),  // 10
	edge(vertices[8], vertices[11], "Piloted", nil), // 11

	edge(vertices[0], vertices[18], "AppearedIn", nil), // 12
	edge(vertices[0], vertices[19], "AppearedIn", nil), // 13
	edge(vertices[0], vertices[20], "AppearedIn", nil), // 14
	edge(vertices[1], vertices[18], "AppearedIn", nil), // 15
	edge(vertices[1], vertices[19], "AppearedIn", nil), // 16
	edge(vertices[1], vertices[20], "AppearedIn", nil), // 17
	edge(vertices[2], vertices[18], "AppearedIn", nil), // 18
	edge(vertices[2], vertices[19], "AppearedIn", nil), // 19
	edge(vertices[2], vertices[20], "AppearedIn", nil), // 20
	edge(vertices[3], vertices[18], "AppearedIn", nil), // 21
	edge(vertices[3], vertices[19], "AppearedIn", nil), // 22
	edge(vertices[3], vertices[20], "AppearedIn", nil), // 23
	edge(vertices[4], vertices[18], "AppearedIn", nil), // 24
	edge(vertices[4], vertices[19], "AppearedIn", nil), // 25
	edge(vertices[4], vertices[20], "AppearedIn", nil), // 26
	edge(vertices[5], vertices[18], "AppearedIn", nil), // 27
	edge(vertices[5], vertices[19], "AppearedIn", nil), // 28
	edge(vertices[5], vertices[20], "AppearedIn", nil), // 29
	edge(vertices[6], vertices[19], "AppearedIn", nil), // 30
	edge(vertices[6], vertices[20], "AppearedIn", nil), // 31
	edge(vertices[7], vertices[18], "AppearedIn", nil), // 32
	edge(vertices[7], vertices[19], "AppearedIn", nil), // 33
	edge(vertices[7], vertices[20], "AppearedIn", nil), // 34
	edge(vertices[8], vertices[18], "AppearedIn", nil), // 35
	edge(vertices[8], vertices[19], "AppearedIn", nil), // 36
	edge(vertices[8], vertices[20], "AppearedIn", nil), // 37

	edge(vertices[9], vertices[18], "AppearedIn", nil),  // 38
	edge(vertices[9], vertices[19], "AppearedIn", nil),  // 39
	edge(vertices[9], vertices[20], "AppearedIn", nil),  // 40
	edge(vertices[10], vertices[18], "AppearedIn", nil), // 41
	edge(vertices[11], vertices[18], "AppearedIn", nil), // 42
	edge(vertices[11], vertices[19], "AppearedIn", nil), // 43
	edge(vertices[11], vertices[20], "AppearedIn", nil), // 44
	edge(vertices[12], vertices[18], "AppearedIn", nil), // 45

	edge(vertices[13], vertices[18], "AppearedIn", nil), // 46
	edge(vertices[13], vertices[20], "AppearedIn", nil), // 47
	edge(vertices[14], vertices[18], "AppearedIn", nil), // 48
	edge(vertices[17], vertices[19], "AppearedIn", nil), // 49
	edge(vertices[17], vertices[20], "AppearedIn", nil), // 50
}

func vertex(label string, d data) *aql.Vertex {
	return &aql.Vertex{
		Gid:   label + "-" + util.UUID(),
		Label: label,
		Data:  protoutil.AsStruct(d),
	}
}

func edge(from, to *aql.Vertex, label string, d data) *aql.Edge {
	return &aql.Edge{
		Gid:   label + "-" + util.UUID(),
		From:  from.Gid,
		To:    to.Gid,
		Label: label,
		Data:  protoutil.AsStruct(d),
	}
}

type data map[string]interface{}

func writeCSV(headers []string, values [][]string, prefix string) (string, error) {
	tmpfile, err := ioutil.TempFile(".", prefix)
	if err != nil {
		return "", fmt.Errorf("csv convert: creating tempfile: %v", err)
	}
	defer tmpfile.Close()
	w := csv.NewWriter(tmpfile)
	if err := w.Write(headers); err != nil {
		return "", fmt.Errorf("csv convert: writing header: %v", err)
	}
	if err := w.WriteAll(values); err != nil {
		return "", fmt.Errorf("csv convert: writing data: %v", err)
	}
	return tmpfile.Name(), nil
}

func split(in interface{}) interface{} {
	var o interface{}
	switch v := in.(type) {
	case []*aql.Vertex:
		out := [][]*aql.Vertex{}
		batch := []*aql.Vertex{}
		lastLabel := v[0].Label
		for _, i := range v {
			if lastLabel != i.Label {
				out = append(out, batch)
				batch = []*aql.Vertex{}
			}
			batch = append(batch, i)
			lastLabel = i.Label
		}
		out = append(out, batch)
		o = out

	case []*aql.Edge:
		out := [][]*aql.Edge{}
		batch := []*aql.Edge{}
		lastLabel := v[0].Label
		for _, i := range v {
			if lastLabel != i.Label {
				out = append(out, batch)
				batch = []*aql.Edge{}
			}
			batch = append(batch, i)
			lastLabel = i.Label
		}
		out = append(out, batch)
		o = out

	default:
		panic(fmt.Errorf("unknown type: %T", in))
	}

	return o
}

func getHeader(in interface{}, schema *sql.Schema) []string {
	out := []string{}
	set := map[string]interface{}{}
	switch v := in.(type) {
	case []*aql.Vertex:
		for _, i := range v {
			data := addForeignKeysToVertex(i, schema)
			keys := config.GetKeys(data)
			for _, k := range keys {
				set[k] = nil
			}
		}

	case []*aql.Edge:
		for _, e := range v {
			data := addForeignKeysToEdge(e, schema)
			keys := config.GetKeys(data)
			for _, k := range keys {
				set[k] = nil
			}
		}

	default:
		panic(fmt.Errorf("unknown type: %T", in))
	}
	for k := range set {
		out = append(out, k)
	}
	return out
}

func addForeignKeysToVertex(vert *aql.Vertex, schema *sql.Schema) map[string]interface{} {
	data := protoutil.AsMap(vert.Data)
	data["id"] = vert.Gid
	for _, es := range schema.Edges {
		if es.Table != "" {
			continue
		}
		if es.To.DestTable == vert.Label {
			if _, ok := data[es.To.DestField]; !ok {
				data[es.To.DestField] = ""
			}
		}
		if es.From.DestTable == vert.Label {
			if _, ok := data[es.From.DestField]; !ok {
				data[es.From.DestField] = ""
			}
		}
	}
	return data
}

func addForeignKeyValuesToVertex(vert *aql.Vertex, edges []*aql.Edge, schema *sql.Schema) map[string]interface{} {
	data := protoutil.AsMap(vert.Data)
	data["id"] = vert.Gid
	for _, es := range schema.Edges {
		if es.Table != "" {
			continue
		}
		for _, e := range edges {
			fromLabel := strings.Split(e.From, "-")[0]
			if es.Label == e.Label && es.From.DestTable == fromLabel {
				if e.To == data["id"] {
					if _, ok := data[es.To.DestField]; !ok {
						data[es.To.DestField] = e.From
					}
				}
			}
			toLabel := strings.Split(e.To, "-")[0]
			if es.Label == e.Label && es.To.DestTable == toLabel {
				if e.From == data["id"] {
					if _, ok := data[es.From.DestField]; !ok {
						data[es.From.DestField] = e.To
					}
				}
			}
		}
	}
	return data
}

func addForeignKeysToEdge(edge *aql.Edge, schema *sql.Schema) map[string]interface{} {
	data := protoutil.AsMap(edge.Data)
	for _, es := range schema.Edges {
		if es.Table == "" || es.Label != edge.Label {
			continue
		}

		if _, ok := data[es.From.SourceField]; !ok {
			data[es.From.SourceField] = ""
		}
		if _, ok := data[es.To.SourceField]; !ok {
			data[es.To.SourceField] = ""
		}
	}
	if len(data) > 0 {
		data["id"] = edge.Gid
	}
	return data
}

func addForeignKeyValuesToEdge(edge *aql.Edge, schema *sql.Schema) map[string]interface{} {
	data := protoutil.AsMap(edge.Data)
	for _, es := range schema.Edges {
		if es.Table == "" || es.Label != edge.Label {
			continue
		}
		if _, ok := data[es.From.SourceField]; !ok {
			data[es.From.SourceField] = edge.From
		}
		if _, ok := data[es.To.SourceField]; !ok {
			data[es.To.SourceField] = edge.To
		}
	}
	if len(data) > 0 {
		data["id"] = edge.Gid
	}
	return data
}

func verticesToCSV(verts []*aql.Vertex, edges []*aql.Edge, schema *sql.Schema) ([]string, error) {
	fnames := []string{}

	sorted := make([]*aql.Vertex, len(verts))
	copy(sorted, verts)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Label < sorted[j].Label
	})

	batches := split(sorted).([][]*aql.Vertex)
	for _, batch := range batches {
		header := getHeader(batch, schema)
		label := batch[0].Label
		values := [][]string{}
		for _, vert := range batch {
			data := addForeignKeyValuesToVertex(vert, edges, schema)
			vals := []string{}
			for _, key := range header {
				vals = append(vals, fmt.Sprintf("%v", data[key]))
			}
			if len(vals) != len(header) {
				return nil, fmt.Errorf("csv convert: data contains nested fields")
			}
			values = append(values, vals)
		}
		prefix := label + "_vertices_"
		f, err := writeCSV(header, values, prefix)
		if err != nil {
			return nil, err
		}
		fnames = append(fnames, f)
	}
	return fnames, nil
}

func edgesToCSV(edges []*aql.Edge, schema *sql.Schema) ([]string, error) {
	fnames := []string{}

	sorted := make([]*aql.Edge, len(edges))
	copy(sorted, edges)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Label < sorted[j].Label
	})

	batches := split(sorted).([][]*aql.Edge)
	for _, batch := range batches {
		header := getHeader(batch, schema)
		if len(header) == 0 {
			continue
		}
		label := batch[0].Label
		values := [][]string{}
		for _, e := range batch {
			data := addForeignKeyValuesToEdge(e, schema)
			vals := []string{}
			for _, key := range header {
				vals = append(vals, fmt.Sprintf("%v", data[key]))
			}
			if len(vals) != len(header) {
				return nil, fmt.Errorf("csv convert: data contains nested fields")
			}
			values = append(values, vals)
		}
		prefix := label + "_edges_"
		f, err := writeCSV(header, values, prefix)
		if err != nil {
			return nil, err
		}
		fnames = append(fnames, f)
	}
	return fnames, nil
}
