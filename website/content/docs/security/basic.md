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

## Using the Python Client

Some GRIP servers may require authorizaiton to access its API endpoints. The client can be configured to pass
authorization headers in its requests:

```python
import gripql

# Basic Auth Header - {'Authorization': 'Basic dGVzdDpwYXNzd29yZA=='}
G = gripql.Connection("https://bmeg.io", user="test", password="password").graph("bmeg")
```

Although GRIP only supports basic password authentication, some servers may be proctected via a nginx or apache 
server. The python client can be configured to handle these cases as well:

```python
import gripql 

# Bearer Token - {'Authorization': 'Bearer iamnotarealtoken'}
G = gripql.Connection("https://bmeg.io", token="iamnotarealtoken").graph("bmeg")

# OAuth2 / Custom - {"OauthEmail": "fake.user@gmail.com", "OauthAccessToken": "iamnotarealtoken", "OauthExpires": 1551985931}
G = gripql.Connection("https://bmeg.io",  credential_file="~/.grip_token.json").graph("bmeg")
```
