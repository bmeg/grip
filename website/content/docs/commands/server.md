---
title: server
menu:
  main:
    parent: commands
    weight: 1
---

# `server`
The server command starts up a graph server and waits for incoming requests.

## Default Configuration
If invoked with no arguments or config files, GRIP will start up in embedded mode, using a Badger based graph driver.

## Networking
By default the GRIP server operates on 2 ports, `8201` is the HTTP based interface. Port `8202` is a GRPC based interface. Python, R and Javascript clients are designed to connect to the HTTP interface on `8201`. The `grip` command will often use port `8202` in order to complete operations. For example if you call `grip list graphs` it will contact port `8202`, rather then using the HTTP port. This means that if you are working with a server that is behind a firewall, and only the HTTP port is available, then the grip command line program will not be able to issue commands, even if the server is visible to client libraries.

## CLI Usage
The `server` command can take several flags for configuration:
- `--config` or `-c` - Specifies a YAML config file with server settings. This overwrites all other settings. Defaults to "" (empty string).
- `--http-port` - Sets the port used by the HTTP interface. Defaults to "8201".
- `--rpc-port` - Sets the port used by the GRPC interface. Defaults to "8202".
- `--read-only` - Start server in read-only mode. Defaults to false.
- `--log-level` or `--log-format` - Set logging level and format, respectively. Defaults are "info" for log level and "text" for format.
- `--log-requests` - Log all requests. Defaults to false.
- `--verbose` - Sets the log level to debug if true.
- `--plugins` or `-p` - Specifies a directory with GRIPPER plugins to load. If not specified, no plugins will be loaded by default.
- `--driver` or `-d` - Specifies the default driver for graph storage. Defaults to "badger". Other possible options are: "pebble", "mongo", "grids", and "sqlite".
- `--endpoint` or `-w` - Load a web endpoint plugin. Use multiple times to load multiple plugins. The format is key=value where key is the plugin name and value is the configuration string for the plugin.
- `--endpoint-config` or `-l` - Configure a loaded web endpoint plugin. Use multiple times to configure multiple plugins. The format is key=value where key is in the form 'pluginname:key' and value is the configuration value for that key.
- `--er` or `-e` - Set GRIPPER source addresses. This flag can be used multiple times to specify multiple addresses. Defaults to an empty map.

## Examples

```bash
# Load server with a specific config file
grip server --config /path/to/your_config.yaml

# Set the HTTP port to 9001
grip server --http-port 9001

# Start in read-only mode
grip server --read-only

# Enable verbose logging (sets log level to debug)
grip server --verbose

# Load a web endpoint plugin named 'foo' with configuration string 'config=value'
grip server --endpoint foo=config=value

# Configure the loaded 'foo' web endpoint plugin, setting its key 'key1' to value 'val1'
grip server --endpoint-config foo:key1=val1
```
