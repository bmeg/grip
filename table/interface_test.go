package table_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bmeg/grip/table"
)

type fakeTable struct {
	collections []table.Collection
	rows        map[string]map[string]*table.Row
}

func (f *fakeTable) Capabilities() table.Capabilities {
	return table.Capabilities{Readable: true}
}

func (f *fakeTable) ListCollections(context.Context) ([]table.Collection, error) {
	return f.collections, nil
}

func (f *fakeTable) GetCollection(_ context.Context, name string) (table.Collection, error) {
	for _, collection := range f.collections {
		if collection.Name == name {
			return collection, nil
		}
	}
	return table.Collection{}, table.ErrCollectionNotFound
}

func (f *fakeTable) Scan(ctx context.Context, collection string, _ table.ScanOptions) (table.RowStream, error) {
	if _, ok := f.rows[collection]; !ok {
		return nil, table.ErrCollectionNotFound
	}
	out := make(chan table.RowResult)
	go func() {
		defer close(out)
		if err := ctx.Err(); err != nil {
			out <- table.RowResult{Err: err}
			return
		}
		for _, row := range f.rows[collection] {
			select {
			case <-ctx.Done():
				out <- table.RowResult{Err: ctx.Err()}
				return
			case out <- table.RowResult{Row: row}:
			}
		}
	}()
	return out, nil
}

func (f *fakeTable) LookupByID(_ context.Context, collection, id string) (*table.Row, error) {
	if rows, ok := f.rows[collection]; ok {
		if row, ok := rows[id]; ok {
			return row, nil
		}
		return nil, table.ErrRowNotFound
	}
	return nil, table.ErrCollectionNotFound
}

func (f *fakeTable) LookupByField(ctx context.Context, collection, field string, value interface{}) (table.RowStream, error) {
	out, err := f.Scan(ctx, collection, table.ScanOptions{})
	if err != nil {
		return nil, err
	}
	filtered := make(chan table.RowResult)
	go func() {
		defer close(filtered)
		for result := range out {
			if result.Err != nil {
				filtered <- result
				continue
			}
			if result.Row.Data[field] == value {
				filtered <- result
			}
		}
	}()
	return filtered, nil
}

func TestTableDriverContract(t *testing.T) {
	f := &fakeTable{
		collections: []table.Collection{{
			Name:       "people",
			PrimaryKey: "id",
			Fields: []table.Field{
				{Name: "id", Type: table.FieldTypeString, Indexed: true},
				{Name: "name", Type: table.FieldTypeString},
			},
		}},
		rows: map[string]map[string]*table.Row{
			"people": {
				"1": {ID: "1", Data: map[string]interface{}{"id": "1", "name": "Luke"}},
				"2": {ID: "2", Data: map[string]interface{}{"id": "2", "name": "Leia"}},
			},
		},
	}

	var _ table.TableDriver = f

	collections, err := f.ListCollections(context.Background())
	if err != nil || len(collections) != 1 {
		t.Fatalf("ListCollections() = %v, %v", collections, err)
	}
	collection, err := f.GetCollection(context.Background(), "people")
	if err != nil {
		t.Fatal(err)
	}
	if err := table.ValidateCollection(collection); err != nil {
		t.Fatal(err)
	}

	row, err := f.LookupByID(context.Background(), "people", "1")
	if err != nil || row.Data["name"] != "Luke" {
		t.Fatalf("LookupByID() = %#v, %v", row, err)
	}
	if _, err := f.LookupByID(context.Background(), "people", "missing"); !errors.Is(err, table.ErrRowNotFound) {
		t.Fatalf("LookupByID missing error = %v", err)
	}

	matches, err := f.LookupByField(context.Background(), "people", "name", "Luke")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for result := range matches {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
		count++
	}
	if count != 1 {
		t.Fatalf("LookupByField returned %d rows", count)
	}
}

func TestScanCancellation(t *testing.T) {
	f := &fakeTable{
		collections: []table.Collection{{Name: "people", PrimaryKey: "id"}},
		rows: map[string]map[string]*table.Row{
			"people": {"1": {ID: "1", Data: map[string]interface{}{"id": "1"}}},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream, err := f.Scan(ctx, "people", table.ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for result := range stream {
		if !errors.Is(result.Err, context.Canceled) {
			t.Fatalf("Scan cancellation error = %v", result.Err)
		}
	}
}

func TestTableValidation(t *testing.T) {
	if err := table.ValidateCollection(table.Collection{Name: "x", PrimaryKey: "missing"}); !errors.Is(err, table.ErrInvalidRequest) {
		t.Fatalf("ValidateCollection() = %v", err)
	}
	if err := table.ValidateScanOptions(table.ScanOptions{Limit: -1}); !errors.Is(err, table.ErrInvalidRequest) {
		t.Fatalf("ValidateScanOptions() = %v", err)
	}
	for _, row := range []*table.Row{
		nil,
		{Data: map[string]interface{}{}},
		{ID: "1"},
	} {
		if err := table.ValidateRow(row); !errors.Is(err, table.ErrMalformedRow) {
			t.Fatalf("ValidateRow(%#v) = %v", row, err)
		}
	}
}
