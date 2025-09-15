---
title: delete
menu:
  main:
    parent: commands
    weight: 3
---

# `delete` Command

## Usage

```bash
gripql-cli delete <graph> --host <url> --file <path> --edges <edge_ids> --vertices <vertex_ids>
```

### Options

- `<graph>`: Name of the graph (required)
- `--host <url>`: GripQL server URL (default: "localhost:8202")
- `--file <path>`: Path to a JSON file containing data to delete
- `--edges <edge_ids>`: Comma-separated list of edge IDs to delete (ignored if JSON file is provided)
- `--vertices <vertex_ids>`: Comma-separated list of vertex IDs to delete (ignored if JSON file is provided)

## Example

```bash
gripql-cli delete my_graph --host myserver.com:8202 --edges edge1,edge2 --vertices vertex3,vertex4
```

## JSON File Format

JSON file format for data to be deleted:

```json
{
 "graph": "graph_name",
 "edges": ["list of edge ids"],
 "vertices": ["list of vertex ids"]
}
```

