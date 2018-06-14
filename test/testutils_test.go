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
)

var people_verts = []*aql.Vertex{
	vertex(0, "Character", data{"name": "Obi-Wan Kenobi", "height": 182, "occupation": "jedi", "species": "human", "starship_id": [], "film_id": [18, 19, 20], "planet_id": [13]}),
	vertex(1, "Character", data{"name": "Luke Skywalker", "height": 172, "occupation": "jedi", "species": "human", "starship_id": [11], "film_id": [18, 19, 20], "planet_id": [13]}),
	vertex(2, "Character", data{"name": "Han Solo", "height": 180, "occupation": "smuggler", "species": "human", "starship_id": [9], "film_id": [18, 19, 20], "planet_id": [17]}),
	vertex(3, "Character", data{"name": "Leia Organa", "height": 150, "occupation": "politician", "species": "human", "starship_id": [], "film_id": [18, 19, 20], "planet_id": [14]}),
	vertex(4, "Character", data{"name": "Darth Vader", "height": 202, "occupation": "sith", "species": "human", "starship_id": [12], "film_id": [18, 19, 20], "planet_id": [13]}),
	vertex(5, "Character", data{"name": "Chewbacca", "height": 228, "occupation": "smuggler", "species": "wookie", "starship_id": [9], "film_id": [18, 19, 20], "planet_id": [16]}),
	vertex(6, "Character", data{"name": "Yoda", "height": 66, "occupation": "jedi", "species": nil, "starship_id": [], "film_id": [19, 20], "planet_id": [15]}),
	vertex(7, "Character", data{"name": "C-3PO", "height": 167, "occupation": "translator", "species": "droid", "starship_id": [], "film_id": [18, 19, 20], "planet_id": []}),
	vertex(8, "Character", data{"name": "R2-D2", "height": 96, "occupation": "starship mechanic", "species": "droid", "starship_id": [], "film_id": [18, 19, 20], "planet_id": []}),

	vertex(9, "Starship", data{"name": "Millennium Falcon", "model": "YT-1300 light freighter", "film_id": [18, 19, 20]}),
	vertex(10, "Starship", data{"name": "Death Star", "model": "DS-1 Orbital Battle Station", "film_id": [18]}),
	vertex(11, "Starship", data{"name": "X-wing", "model": "T-65 X-wing", "film_id": [18, 19, 20]}),
	vertex(12, "Starship", data{"name": "TIE Advanced x1", "model": "Twin Ion Engine Advanced x1", "film_id": [18]}),

	vertex(13, "Planet", data{"name": "Tatooine", "diameter": 10465, "population": 200000, "film_id": [18, 20]}),
	vertex(14, "Planet", data{"name": "Alderan", "diameter": 12500, "population": 2000000000, "film_id": [18]}),
	vertex(15, "Planet", data{"name": "Dagobah", "diameter": 8900, "population": nil, "film_id": [19, 20]}),
	vertex(16, "Planet", data{"name": "Kashyyyk", "diameter": 12765, "population": 45000000, "film_id": []}),
	vertex(17, "Planet", data{"name": "Corellia", "diameter": 11000, "population": 3000000000, "film_id": []}),

	vertex(18, "Film", data{"title": "A New Hope", "episode": "IV", "starship_id": [9, 10, 11, 12], "character_id": [0, 1, 2, 3, 4, 5,  7, 8], "planet_id": [13, 13]}),
	vertex(19, "Film", data{"title": "Empire Strikes Back", "episode": "V", "starship_id": [9, 11], "character_id": [0, 1, 2, 3, 4, 5, 6, 7, 8], "planet_id": [15]}),
	vertex(20, "Film", data{"title": "Return of the Jedi", "episode": "VI", "starship_id": [9, 11], "character_id": [0, 1, 2, 3, 4, 5, 6, 7, 8], "planet_id": [13, 15]}),
}

var edges = []*aql.Edge{
	edge(0, vertices[0], vertices[13], "LivedOn", data{"homeworld": false}),
	edge(1, vertices[1], vertices[13], "LivedOn", data{"homeworld": true}),
	edge(2, vertices[2], vertices[17], "LivedOn", data{"homeworld": true}),
	edge(3, vertices[3], vertices[14], "LivedOn", data{"homeworld": true}),
	edge(4, vertices[4], vertices[13], "LivedOn", data{"homeworld": true}),
	edge(5, vertices[5], vertices[16], "LivedOn", data{"homeworld": true}),
	edge(6, vertices[6], vertices[15], "LivedOn", data{"homeworld": false}),

	edge(7, vertices[1], vertices[11], "Piloted", nil),
	edge(8, vertices[2], vertices[9], "Piloted", nil),
	edge(9, vertices[4], vertices[12], "Piloted", nil),
	edge(10, vertices[5], vertices[9], "Piloted", nil),
	edge(11, vertices[8], vertices[11], "Piloted", nil),

	edge(12, vertices[0], vertices[18], "AppearedIn", nil),
	edge(13, vertices[0], vertices[19], "AppearedIn", nil),
	edge(14, vertices[0], vertices[20], "AppearedIn", nil),
	edge(15, vertices[1], vertices[18], "AppearedIn", nil),
	edge(16, vertices[1], vertices[19], "AppearedIn", nil),
	edge(17, vertices[1], vertices[20], "AppearedIn", nil),
	edge(18, vertices[2], vertices[18], "AppearedIn", nil),
	edge(19, vertices[2], vertices[19], "AppearedIn", nil),
	edge(20, vertices[2], vertices[20], "AppearedIn", nil),
	edge(21, vertices[3], vertices[18], "AppearedIn", nil),
	edge(22, vertices[3], vertices[19], "AppearedIn", nil),
	edge(23, vertices[3], vertices[20], "AppearedIn", nil),
	edge(24, vertices[4], vertices[18], "AppearedIn", nil),
	edge(25, vertices[4], vertices[19], "AppearedIn", nil),
	edge(26, vertices[4], vertices[20], "AppearedIn", nil),
	edge(27, vertices[5], vertices[18], "AppearedIn", nil),
	edge(28, vertices[5], vertices[19], "AppearedIn", nil),
	edge(29, vertices[5], vertices[20], "AppearedIn", nil),
	edge(30, vertices[6], vertices[19], "AppearedIn", nil),
	edge(31, vertices[6], vertices[20], "AppearedIn", nil),
	edge(32, vertices[7], vertices[18], "AppearedIn", nil),
	edge(33, vertices[7], vertices[19], "AppearedIn", nil),
	edge(34, vertices[7], vertices[20], "AppearedIn", nil),
	edge(35, vertices[8], vertices[18], "AppearedIn", nil),
	edge(36, vertices[8], vertices[19], "AppearedIn", nil),
	edge(37, vertices[8], vertices[20], "AppearedIn", nil),

	edge(38, vertices[9], vertices[18], "AppearedIn", nil),
	edge(39, vertices[9], vertices[19], "AppearedIn", nil),
	edge(40, vertices[9], vertices[20], "AppearedIn", nil),
	edge(41, vertices[10], vertices[18], "AppearedIn", nil),
	edge(42, vertices[11], vertices[18], "AppearedIn", nil),
	edge(43, vertices[11], vertices[19], "AppearedIn", nil),
	edge(44, vertices[11], vertices[20], "AppearedIn", nil),
	edge(45, vertices[12], vertices[18], "AppearedIn", nil),

	edge(46, vertices[13], vertices[18], "AppearedIn", nil),
	edge(47, vertices[13], vertices[20], "AppearedIn", nil),
	edge(48, vertices[14], vertices[18], "AppearedIn", nil),
	edge(49, vertices[15], vertices[19], "AppearedIn", nil),
	edge(50, vertices[15], vertices[20], "AppearedIn", nil),
}

func vertex(gid interface{}, label string, d data) *aql.Vertex {
	return &aql.Vertex{
		Gid:   fmt.Sprintf("%v", gid),
		Label: label,
		Data:  protoutil.AsStruct(d),
	}
}

func edge(gid interface{}, from, to *aql.Vertex, label string, d data) *aql.Edge {
	return &aql.Edge{
		Gid:   fmt.Sprintf("%v", gid),
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
			data := protoutil.AsMap(i.Data)
      data["id"] = ""
			keys := config.GetKeys(data)
			for _, k := range keys {
				set[k] = nil
			}
		}
	case []*aql.Edge:
		for _, e := range v {
      data := protoutil.AsMap(i.Data)
      if len(data) > 0 {
        data["id"] = ""
      }
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

func verticesToCSV(verts []*aql.Vertex) ([]string, error) {
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
			data := protoutil.AsMap(vert.Data)
      data["id"] = vert.Gid
			vals := []string{}
			for key, _ := range data {
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

func edgesToCSV(edges []*aql.Edge) ([]string, error) {
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
			data := protoutil.AsMap(e.Data)      
      data["id"] = vert.Gid
			vals := []string{}
			for key, _ := range data {
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
