---
title: Basic Auth

menu:
  main:
    parent: Security
    weight: 1
---

# Basic Auth

By default, an Arachne server allows open access to its API endpoints, but it 
can be configured to require basic password authentication. To enable this, 
include users and passwords in your config file:

```yaml
Server:
  BasicAuth:
    - User: testuser
      Password: abc123
```

Make sure to properly protect the configuration file so that it's not readable 
by everyone:

```bash
$ chmod 600 arachne.config.yml
```

To use the password, set the `ARACHNE_USER` and `ARACHNE_PASSWORD` environment variables:
```bash
$ export ARACHNE_USER=testuser
$ export ARACHNE_PASSWORD=abc123
$ arachne list
```
