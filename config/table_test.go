package config_test

import (
	"testing"

	"github.com/bmeg/grip/config"
)

func TestParseTableDriverConfig(t *testing.T) {
	raw := []byte(`
default: table
drivers:
  table:
    table:
      plugin: /tmp/grip-table-example.so
      config:
        name: example
      graphs:
        - graph: example
          mapping:
            vertices:
              - collection: people
                label: Person
                prefix: "Person:"
                id: id
`)
	conf := config.DefaultConfig()
	if err := config.ParseConfig(raw, conf); err != nil {
		t.Fatal(err)
	}
	driver, ok := conf.Drivers["table"]
	if !ok || driver.Table == nil {
		t.Fatalf("table driver not parsed: %#v", conf.Drivers)
	}
	if driver.Table.Plugin == "" || len(driver.Table.Graphs) != 1 {
		t.Fatalf("table config = %#v", driver.Table)
	}
	if driver.Table.Config["name"] != "example" {
		t.Fatalf("table plugin config = %#v", driver.Table.Config)
	}
}

func TestTableConfigRedaction(t *testing.T) {
	raw := []byte(`
default: table
drivers:
  table:
    table:
      plugin: /tmp/plugin.so
      config:
        token: secret
      graphs:
        - graph: example
          mapping:
            vertices: []
`)
	conf := config.DefaultConfig()
	if err := config.ParseConfig(raw, conf); err != nil {
		t.Fatal(err)
	}
	redacted := config.DeepCopyRedactedConfig(conf)
	driver := redacted.Drivers["table"]
	if driver.Table == nil {
		t.Fatal("redacted table config missing")
	}
	if driver.Table.Config["token"] != "[REDACTED]" {
		t.Fatalf("table plugin config was not redacted: %#v", driver.Table.Config)
	}
}
