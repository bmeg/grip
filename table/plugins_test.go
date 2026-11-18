package table_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	griptable "github.com/bmeg/grip/table"
)

func TestDiscoverPlugins(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"b.so", "a.so", "ignored.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("test"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	plugins, err := griptable.DiscoverPlugins(dir)
	if err != nil {
		t.Skipf("dynamic plugins unavailable: %v", err)
	}
	if len(plugins) != 2 || plugins[0].Name != "a" || plugins[1].Name != "b" {
		t.Fatalf("DiscoverPlugins() = %#v", plugins)
	}
	if plugins[0].ABIName != griptable.ABIName || plugins[0].ABIVersion != griptable.ABIVersion {
		t.Fatalf("DiscoverPlugins ABI = %#v", plugins[0])
	}
}

func TestDiscoverPluginsMissingDir(t *testing.T) {
	_, err := griptable.DiscoverPlugins(filepath.Join(t.TempDir(), "missing"))
	if err == nil {
		t.Fatal("DiscoverPlugins missing directory should fail")
	}
}

func TestLoadPluginMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.so")
	_, err := griptable.LoadPlugin(path, nil)
	if err == nil {
		t.Fatal("LoadPlugin missing plugin should fail")
	}
	if !errors.Is(err, griptable.ErrPluginNotFound) && !errors.Is(err, griptable.ErrPluginABI) {
		t.Fatalf("LoadPlugin missing error = %v", err)
	}
}

func TestLoadPluginInvalidModule(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.so")
	if err := os.WriteFile(path, []byte("not a Go plugin"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := griptable.LoadPlugin(path, nil)
	if err == nil {
		t.Fatal("LoadPlugin invalid module should fail")
	}
	if !errors.Is(err, griptable.ErrPluginABI) {
		t.Fatalf("LoadPlugin invalid error = %v", err)
	}
}

func TestDynamicPluginBuildAndLoad(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("dynamic Go plugins are not supported")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "dynamicplugin.so")
	build := exec.Command("go", "build", "-buildmode=plugin", "-o", path, "./testdata/dynamicplugin")
	build.Dir = "."
	output, err := build.CombinedOutput()
	if err != nil {
		t.Fatalf("building dynamic plugin: %v\n%s", err, output)
	}

	loaded, err := griptable.LoadPlugin(path, map[string]interface{}{"name": "Luke"})
	if err != nil {
		t.Fatal(err)
	}
	defer loaded.Close()
	row, err := loaded.Driver.LookupByID(context.Background(), "people", "1")
	if err != nil || row.Data["name"] != "Luke" {
		t.Fatalf("dynamic plugin LookupByID() = %#v, %v", row, err)
	}

	config := griptable.Config{
		Plugin: path,
		Config: map[string]interface{}{"name": "Luke"},
		Graphs: []griptable.GraphConfig{{
			Graph: "standalone",
			Mapping: griptable.Mapping{
				Vertices: []griptable.VertexTable{{
					Collection: "people",
					Label:      "Person",
					Prefix:     "Person:",
					ID:         "id",
				}},
			},
		}},
	}
	db, err := griptable.NewConfiguredGraphDB(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	graph, err := db.Graph("standalone")
	if err != nil {
		t.Fatal(err)
	}
	vertex := graph.GetVertex("Person:1", true)
	if vertex == nil || vertex.Data["name"] != "Luke" {
		t.Fatalf("configured dynamic-plugin graph vertex = %#v", vertex)
	}
}
