---
title: list

menu:
  main:
    parent: commands
    weight: 3
---

The `list tables` command is used to display all available tables in the grip server. Each table is represented by its source, name, fields, and link map. Here's a breakdown of how to use this command:

- **Usage:** `gripql list tables`
- **Short Description:** List all available tables in the grip server.
- **Long Description:** This command connects to the grip server and retrieves information about all available tables. It then prints each table's source, name, fields, and link map to the console.
- **Arguments:** None
- **Flags:**
  - `--host`: The URL of the grip server (default: "localhost:8202")

## `gripql list graphs` Command Documentation

The `list graphs` command is used to display all available graphs in the grip server. Here's a breakdown of how to use this command:

- **Usage:** `gripql list graphs`
- **Short Description:** List all available graphs in the grip server.
- **Long Description:** This command connects to the grip server and retrieves information about all available graphs. It then prints each graph's name to the console.
- **Arguments:** None
- **Flags:**
  - `--host`: The URL of the grip server (default: "localhost:8202")

## `gripql list labels` Command Documentation

The `list labels` command is used to display all available vertex and edge labels in a specific graph. Here's a breakdown of how to use this command:

- **Usage:** `gripql list labels <graph>`
- **Short Description:** List the vertex and edge labels in a specific graph.
- **Long Description:** This command takes one argument, the name of the graph, and connects to the grip server. It retrieves information about all available vertex and edge labels in that graph and prints them to the console in JSON format.
- **Arguments:**
  - `<graph>`: The name of the graph to list labels for.
- **Flags:**
  - `--host`: The URL of the grip server (default: "localhost:8202")