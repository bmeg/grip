package main

import (
	"context"

	griptable "github.com/bmeg/grip/table"
)

type pluginTable struct {
	rows map[string]map[string]*griptable.Row
}

func (*pluginTable) Capabilities() griptable.Capabilities {
	return griptable.Capabilities{Readable: true}
}

func (*pluginTable) ListCollections(context.Context) ([]griptable.Collection, error) {
	return []griptable.Collection{{Name: "people", PrimaryKey: "id",
		Fields: []griptable.Field{{Name: "id", Indexed: true}}}}, nil
}

func (d *pluginTable) GetCollection(_ context.Context, name string) (griptable.Collection, error) {
	if name != "people" {
		return griptable.Collection{}, griptable.ErrCollectionNotFound
	}
	return griptable.Collection{Name: "people", PrimaryKey: "id",
		Fields: []griptable.Field{{Name: "id", Indexed: true}}}, nil
}

func (d *pluginTable) Scan(ctx context.Context, collection string, _ griptable.ScanOptions) (griptable.RowStream, error) {
	if collection != "people" {
		return nil, griptable.ErrCollectionNotFound
	}
	out := make(chan griptable.RowResult)
	go func() {
		defer close(out)
		for _, row := range d.rows[collection] {
			select {
			case <-ctx.Done():
				out <- griptable.RowResult{Err: ctx.Err()}
				return
			case out <- griptable.RowResult{Row: row}:
			}
		}
	}()
	return out, nil
}

func (d *pluginTable) LookupByID(_ context.Context, collection, id string) (*griptable.Row, error) {
	row, ok := d.rows[collection][id]
	if !ok {
		return nil, griptable.ErrRowNotFound
	}
	return row, nil
}

func (d *pluginTable) LookupByField(ctx context.Context, collection, field string, value interface{}) (griptable.RowStream, error) {
	stream, err := d.Scan(ctx, collection, griptable.ScanOptions{})
	if err != nil {
		return nil, err
	}
	out := make(chan griptable.RowResult)
	go func() {
		defer close(out)
		for result := range stream {
			if result.Row != nil && result.Row.Data[field] == value {
				out <- result
			}
		}
	}()
	return out, nil
}

// NewTableDriver is the dynamic plugin constructor ABI.
func NewTableDriver(config map[string]interface{}) (griptable.TableDriver, error) {
	name := "Plugin"
	if value, ok := config["name"].(string); ok {
		name = value
	}
	return &pluginTable{
		rows: map[string]map[string]*griptable.Row{
			"people": {
				"1": {ID: "1", Data: map[string]interface{}{"id": "1", "name": name}},
			},
		},
	}, nil
}
