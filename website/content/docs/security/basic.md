---
title: Basic Auth

menu:
  main:
    parent: Security
    weight: 1
---

# Basic Auth

By default, an GRIP server allows open access to its API endpoints, but it
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
$ chmod 600 grip.config.yml
```

To use the password, set the `GRIP_USER` and `GRIP_PASSWORD` environment variables:
```bash
$ export GRIP_USER=testuser
$ export GRIP_PASSWORD=abc123
$ grip list
```
