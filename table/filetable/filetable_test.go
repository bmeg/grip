package filetable_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	griptable "github.com/bmeg/grip/table"
	"github.com/bmeg/grip/table/filetable"
)

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestConformanceFileTable(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "swapi.vertices", `{"_id":"Person:1","_label":"Person","name":"Luke"}
{"_id":"Person:2","_label":"Person","name":"Leia"}
{"_id":"Planet:1","_label":"Planet","name":"Tatooine"}
`)
	writeFile(t, dir, "swapi.edges", `{"_id":"Person:1-knows-Person:2","_label":"knows","_from":"Person:1","_to":"Person:2","weight":1}
`)

	driver, err := filetable.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	collections, err := driver.ListCollections(context.Background())
	if err != nil || len(collections) != 2 {
		t.Fatalf("ListCollections() = %v, %v", collections, err)
	}
	row, err := driver.LookupByID(context.Background(), "swapi.vertices", "Person:1")
	if err != nil || row.Data["name"] != "Luke" {
		t.Fatalf("LookupByID() = %#v, %v", row, err)
	}

	mapping, err := driver.DiscoverMapping("swapi")
	if err != nil {
		t.Fatal(err)
	}
	if len(mapping.Vertices) != 2 || len(mapping.Edges) != 1 {
		t.Fatalf("DiscoverMapping() = %#v", mapping)
	}

	graph, err := driver.NewGraph(context.Background(), "swapi")
	if err != nil {
		t.Fatal(err)
	}
	if vertex := graph.GetVertex("Person:1", true); vertex == nil || vertex.Data["name"] != "Luke" {
		t.Fatalf("Person:1 = %#v", vertex)
	}
	if edge := graph.GetEdge("Person:1-knows-Person:2", true); edge == nil {
		t.Fatal("knows edge missing")
	}
}

func TestConformanceFileTableMissing(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "broken.jsonl", `{"id":"1","name":"only"}
`)
	driver, err := filetable.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.LookupByID(context.Background(), "missing", "1"); err == nil {
		t.Fatal("LookupByID missing collection should fail")
	}
	if _, err := driver.DiscoverMapping("broken"); err == nil {
		t.Fatal("DiscoverMapping missing .vertices/.edges should fail")
	}
}

func TestConformanceFileTableScanCursorAndCancellation(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "rows.jsonl", `{"id":"1","name":"Luke"}
{"id":"2","name":"Leia"}
{"id":"3","name":"Han"}
`)
	driver, err := filetable.New(dir)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := driver.Scan(context.Background(), "rows.jsonl", griptable.ScanOptions{Cursor: "1", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for result := range stream {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
		count++
	}
	if count != 1 {
		t.Fatalf("limited cursor scan returned %d rows", count)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := driver.LookupByID(ctx, "rows.jsonl", "1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("LookupByID cancellation = %v", err)
	}

	stream, err = driver.Scan(ctx, "rows.jsonl", griptable.ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	result := <-stream
	if !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("Scan cancellation = %#v", result)
	}
}

func TestConformanceFileTableMalformedJSON(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "broken.jsonl", "{not json}\n")
	if _, err := filetable.New(dir); err == nil {
		t.Fatal("New malformed JSON should fail")
	}
}

func TestConformanceFileTableQuery(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "swapi.vertices", `{"_id":"Person:1","_label":"Person","name":"Luke"}
{"_id":"Person:2","_label":"Person","name":"Leia"}
{"_id":"Planet:1","_label":"Planet","name":"Tatooine"}
`)
	writeFile(t, dir, "swapi.edges", `{"_id":"Person:1-homeworld-Planet:1","_label":"homeworld","_from":"Person:1","_to":"Planet:1"}
`)
	driver, err := filetable.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	graph, err := driver.NewGraph(context.Background(), "swapi")
	if err != nil {
		t.Fatal(err)
	}
	query := gripql.NewQuery().V("Person:1").Out("homeworld")
	pipe, err := graph.Compiler().Compile(query.Statements, nil)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for result := range pipeline.Run(context.Background(), pipe, t.TempDir()) {
		if result.GetVertex() == nil || result.GetVertex().Id != "Planet:1" {
			t.Fatalf("query result = %#v", result)
		}
		count++
	}
	if count != 1 {
		t.Fatalf("query returned %d results", count)
	}
}

func TestRepositorySWAPIConformanceFiles(t *testing.T) {
	dir := filepath.Join("..", "..", "conformance", "graphs")
	if _, err := os.Stat(filepath.Join(dir, "swapi.vertices")); err != nil {
		t.Skipf("repository SWAPI files unavailable: %v", err)
	}
	driver, err := filetable.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	graph, err := driver.NewGraph(context.Background(), "swapi")
	if err != nil {
		t.Fatal(err)
	}
	if len(graphVertices(graph)) == 0 {
		t.Fatal("repository SWAPI graph has no vertices")
	}
	if labels, err := graph.ListEdgeLabels(); err != nil || len(labels) == 0 {
		t.Fatalf("repository SWAPI edge labels = %v, %v", labels, err)
	}
}

func graphVertices(graph interface {
	GetVertexList(context.Context, bool) <-chan *gdbi.Vertex
}) []string {
	out := []string{}
	for vertex := range graph.GetVertexList(context.Background(), false) {
		out = append(out, vertex.ID)
	}
	return out
}
