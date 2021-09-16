---
title: server

menu:
  main:
    parent: commands
    weight: 1
---

```
grip server
```

The server command starts up a graph server and waits for incoming requests.

## Default Configuration
If invoked with no arguments or config files, GRIP will start up in embedded mode,
using a Badger based graph driver.

## Networking
By default the GRIP server operates on 2 ports, `8201` is the HTTP based interface.
Port `8202` is a GRPC based interface. Python, R and Javascript clients are designed
to connect to the HTTP interface on `8201`. The `grip` command will often use
port `8202` in order to complete operations. For example if you call `grip list graphs`
it will contact port `8202`, rather then using the HTTP port. This means that if
you are working with a server that is behind a firewall, and only the HTTP port is
available, then the grip command line program will not be able to issue commands,
even if the server is visible to client libraries.
