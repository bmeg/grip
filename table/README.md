# GRIP Table Drivers

The top-level `table` package defines a generic Go interface for collection-backed graph data.
It is independent of GRIPPER, gRPC, and `gdbi.GraphInterface`.

## Implement a table driver

A driver implements `table.TableDriver`:

- `Capabilities`
- `ListCollections`
- `GetCollection`
- `Scan`
- `LookupByID`
- `LookupByField`

Use `BatchTableDriver` for batched ID lookup and `WriteTableDriver` for mutation.
Drivers should return typed sentinel errors such as `table.ErrRowNotFound`,
`table.ErrCollectionNotFound`, and `table.ErrUnsupported`.

## Build a graph

```go
graph, err := table.NewGraph(ctx, driver, table.Mapping{
    Vertices: []table.VertexTable{{
        Collection: "people",
        Label: "Person",
        Prefix: "Person:",
        ID: "id",
    }},
})
```

Use `NewGraphDB` to register multiple named graphs backed by the same driver.

## Dynamic Go plugins

Plugins built for Linux or macOS can use Go's dynamic-plugin mode:

```bash
go build -buildmode=plugin -o grip-table-example.so ./example
```

The plugin must export:

```go
func NewTableDriver(config map[string]interface{}) (table.TableDriver, error)
```

The host loads it with `table.LoadPlugin(path, config)`. Row scans and lookups
execute in-process and do not use RPC serialization.

Plugin and host builds must use matching Go, platform, architecture, and CGO
settings. Dynamic plugins are loaded for process lifetime and cannot be safely
unloaded while they are in use. HashiCorp subprocess plugins may be used when
process isolation or cross-platform operation is more important than direct data
throughput.

## Conformance files

`table/filetable` provides a read-only example driver for `.vertices`, `.edges`,
and `.jsonl` files:

```go
driver, err := filetable.New("/path/to/conformance/graphs")
mapping, err := driver.DiscoverMapping("swapi")
graph, err := table.NewGraph(ctx, driver, mapping)
```
