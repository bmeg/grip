package test

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"sort"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/config"
	"github.com/bmeg/arachne/protoutil"
	"github.com/bmeg/arachne/sql"
)

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
			for key := range data {
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
			for key := range data {
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
