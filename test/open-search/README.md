

## Setup

In this directory run server:
```
export OPENSEARCH_INITIAL_ADMIN_PASSWORD=Test-Demo-42
docker-compose up
```

Start up server:
```
grip server --config pebble-opensearch.yml
```

Load example graph:
```
grip load example-graph
```

Submit a job:
```
grip job submit example-graph 'V().hasLabel("Movie").in_()'
```