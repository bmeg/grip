// Package filetable provides a read-only table.TableDriver for JSON-lines
// and conformance .vertices/.edges files.
package filetable

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	griptable "github.com/bmeg/grip/table"
)

// Driver reads collection files directly from a directory.
type Driver struct {
	dir         string
	collections map[string]*griptable.Collection
	rows        map[string][]*griptable.Row
}

// New creates a read-only table driver for immediate files in dir.
func New(dir string) (*Driver, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("opening table directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("table path %q is not a directory", dir)
	}
	d := &Driver{
		dir:         dir,
		collections: map[string]*griptable.Collection{},
		rows:        map[string][]*griptable.Row{},
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("listing table directory: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !isTableFile(name) {
			continue
		}
		collectionName := name
		rows, err := readJSONLines(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		fields := collectionFields(rows)
		collection := &griptable.Collection{
			Name:       collectionName,
			PrimaryKey: primaryKey(fields),
			Fields:     fields,
		}
		d.collections[collectionName] = collection
		d.rows[collectionName] = rows
	}
	return d, nil
}

func isTableFile(name string) bool {
	return strings.HasSuffix(name, ".vertices") ||
		strings.HasSuffix(name, ".edges") ||
		strings.HasSuffix(name, ".jsonl")
}

func readJSONLines(path string) ([]*griptable.Row, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening table file %q: %w", path, err)
	}
	defer file.Close()

	rows := []*griptable.Row{}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<20), 1<<28)
	line := 0
	for scanner.Scan() {
		line++
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" {
			continue
		}
		data := map[string]interface{}{}
		if err := json.Unmarshal(scanner.Bytes(), &data); err != nil {
			return nil, fmt.Errorf("parsing %s line %d: %w", path, line, err)
		}
		id := textID(data)
		if id == "" {
			id = strconv.Itoa(line)
		}
		rows = append(rows, &griptable.Row{ID: id, Data: data})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading table file %q: %w", path, err)
	}
	return rows, nil
}

func textID(data map[string]interface{}) string {
	for _, key := range []string{"_id", "id"} {
		if value, ok := data[key]; ok {
			switch v := value.(type) {
			case string:
				return v
			case json.Number:
				return v.String()
			case float64:
				return strconv.FormatFloat(v, 'f', -1, 64)
			default:
				if v != nil {
					return fmt.Sprint(v)
				}
			}
		}
	}
	return ""
}

func primaryKey(fields []griptable.Field) string {
	for _, field := range fields {
		if field.Name == "_id" {
			return "_id"
		}
		if field.Name == "id" {
			return "id"
		}
	}
	return ""
}

func collectionFields(rows []*griptable.Row) []griptable.Field {
	names := map[string]griptable.Field{}
	for _, row := range rows {
		for name, value := range row.Data {
			fieldType := fieldType(value)
			names[name] = griptable.Field{
				Name: name, Type: fieldType,
				Indexed: name == "_id" || name == "id" || name == "_label" ||
					name == "_from" || name == "_to",
			}
		}
	}
	out := make([]griptable.Field, 0, len(names))
	for _, field := range names {
		out = append(out, field)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func fieldType(value interface{}) griptable.FieldType {
	switch value.(type) {
	case nil:
		return griptable.FieldTypeNull
	case string:
		return griptable.FieldTypeString
	case bool:
		return griptable.FieldTypeBoolean
	case float64, float32, json.Number:
		return griptable.FieldTypeNumber
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return griptable.FieldTypeInteger
	case map[string]interface{}:
		return griptable.FieldTypeObject
	case []interface{}:
		return griptable.FieldTypeArray
	default:
		return griptable.FieldTypeUnknown
	}
}

func (d *Driver) Capabilities() griptable.Capabilities {
	return griptable.Capabilities{Readable: true}
}

func (d *Driver) ListCollections(context.Context) ([]griptable.Collection, error) {
	names := make([]string, 0, len(d.collections))
	for name := range d.collections {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]griptable.Collection, 0, len(names))
	for _, name := range names {
		out = append(out, *d.collections[name])
	}
	return out, nil
}

func (d *Driver) GetCollection(_ context.Context, name string) (griptable.Collection, error) {
	collection, ok := d.collections[name]
	if !ok {
		return griptable.Collection{}, griptable.ErrCollectionNotFound
	}
	return *collection, nil
}

func (d *Driver) Scan(ctx context.Context, collection string, options griptable.ScanOptions) (griptable.RowStream, error) {
	if err := griptable.ValidateScanOptions(options); err != nil {
		return nil, err
	}
	rows, ok := d.rows[collection]
	if !ok {
		return nil, griptable.ErrCollectionNotFound
	}
	out := make(chan griptable.RowResult)
	go func() {
		defer close(out)
		if err := ctx.Err(); err != nil {
			out <- griptable.RowResult{Err: err}
			return
		}
		start := 0
		if options.Cursor != "" {
			if cursor, err := strconv.Atoi(options.Cursor); err == nil && cursor >= 0 {
				start = cursor
			}
		}
		if start >= len(rows) {
			return
		}
		sent := 0
		for _, row := range rows[start:] {
			if err := ctx.Err(); err != nil {
				out <- griptable.RowResult{Err: err}
				return
			}
			if options.Limit > 0 && sent >= options.Limit {
				return
			}
			select {
			case <-ctx.Done():
				out <- griptable.RowResult{Err: ctx.Err()}
				return
			case out <- griptable.RowResult{Row: row}:
				sent++
			}
		}
	}()
	return out, nil
}

func (d *Driver) LookupByID(ctx context.Context, collection, id string) (*griptable.Row, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows, ok := d.rows[collection]
	if !ok {
		return nil, griptable.ErrCollectionNotFound
	}
	for _, row := range rows {
		if row.ID == id {
			return row, nil
		}
	}
	return nil, griptable.ErrRowNotFound
}

func (d *Driver) LookupByField(ctx context.Context, collection, field string, value interface{}) (griptable.RowStream, error) {
	rows, ok := d.rows[collection]
	if !ok {
		return nil, griptable.ErrCollectionNotFound
	}
	out := make(chan griptable.RowResult)
	go func() {
		defer close(out)
		for _, row := range rows {
			select {
			case <-ctx.Done():
				out <- griptable.RowResult{Err: ctx.Err()}
				return
			default:
			}
			if fmt.Sprint(row.Data[field]) == fmt.Sprint(value) {
				select {
				case <-ctx.Done():
					out <- griptable.RowResult{Err: ctx.Err()}
					return
				case out <- griptable.RowResult{Row: row}:
				}
			}
		}
	}()
	return out, nil
}

// DiscoverMapping builds a label-aware graph mapping from a conformance
// graphName's .vertices and .edges files.
func (d *Driver) DiscoverMapping(graphName string) (griptable.Mapping, error) {
	mapping := griptable.Mapping{}
	vertexCollection := graphName + ".vertices"
	edgeCollection := graphName + ".edges"

	labels := map[string]bool{}
	if rows, ok := d.rows[vertexCollection]; ok {
		for _, row := range rows {
			label := textField(row.Data["_label"])
			if label == "" {
				continue
			}
			labels[label] = true
		}
	}
	vertexLabels := make([]string, 0, len(labels))
	for label := range labels {
		vertexLabels = append(vertexLabels, label)
	}
	sort.Strings(vertexLabels)
	for _, label := range vertexLabels {
		mapping.Vertices = append(mapping.Vertices, griptable.VertexTable{
			Collection: vertexCollection,
			Label:      label,
			ID:         "_id",
			LabelField: "_label",
			LabelValue: label,
		})
	}

	edgeLabels := map[string]bool{}
	if rows, ok := d.rows[edgeCollection]; ok {
		for _, row := range rows {
			label := textField(row.Data["_label"])
			if label != "" {
				edgeLabels[label] = true
			}
		}
	}
	sortedEdgeLabels := make([]string, 0, len(edgeLabels))
	for label := range edgeLabels {
		sortedEdgeLabels = append(sortedEdgeLabels, label)
	}
	sort.Strings(sortedEdgeLabels)
	for _, label := range sortedEdgeLabels {
		mapping.Edges = append(mapping.Edges, griptable.EdgeTable{
			Collection: edgeCollection,
			Label:      label,
			From:       griptable.Endpoint{Field: "_from"},
			To:         griptable.Endpoint{Field: "_to"},
			LabelField: "_label",
			LabelValue: label,
		})
	}
	if len(mapping.Vertices) == 0 {
		return mapping, fmt.Errorf("conformance graph %q has no vertices in %s", graphName, vertexCollection)
	}
	return mapping, nil
}

func textField(value interface{}) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

// NewGraph creates a standalone table graph for one conformance graph.
func (d *Driver) NewGraph(ctx context.Context, graphName string) (*griptable.Graph, error) {
	mapping, err := d.DiscoverMapping(graphName)
	if err != nil {
		return nil, err
	}
	return griptable.NewGraph(ctx, d, mapping)
}
