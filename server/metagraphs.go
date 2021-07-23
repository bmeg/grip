package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/gripper"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/rpc"
)

var schemaSuffix = "__schema__"
var mappingSuffix = "__mapping__"

func isSchema(graphName string) bool {
	return strings.HasSuffix(graphName, schemaSuffix)
}

func isMapping(graphName string) bool {
	return strings.HasSuffix(graphName, mappingSuffix)
}

func (server *GripServer) getGraph(graph string) (*gripql.Graph, error) {

	conn, err := gripql.Connect(rpc.ConfigWithDefaults(server.conf.Server.RPCAddress()), true)
	if err != nil {
		return nil, fmt.Errorf("failed to load existing schema: %v", err)
	}
	res, err := conn.Traversal(&gripql.GraphQuery{Graph: graph, Query: gripql.NewQuery().V().Statements})
	if err != nil {
		return nil, fmt.Errorf("failed to load existing schema: %v", err)
	}
	vertices := []*gripql.Vertex{}
	for row := range res {
		vertices = append(vertices, row.GetVertex())
	}
	res, err = conn.Traversal(&gripql.GraphQuery{Graph: graph, Query: gripql.NewQuery().E().Statements})
	if err != nil {
		return nil, fmt.Errorf("failed to load existing schema: %v", err)
	}
	edges := []*gripql.Edge{}
	for row := range res {
		edges = append(edges, row.GetEdge())
	}
	return &gripql.Graph{Graph: graph, Vertices: vertices, Edges: edges}, nil
}

func (server *GripServer) buildSchemas(ctx context.Context) {
	for _, gdb := range server.dbs {
		for _, name := range gdb.ListGraphs() {
			select {
			case <-ctx.Done():
				return

			default:
				if isSchema(name) {
					continue
				}
				if _, ok := server.schemas[name]; ok {
					log.WithFields(log.Fields{"graph": name}).Debug("skipping build; cached schema found")
					continue
				}
				log.WithFields(log.Fields{"graph": name}).Debug("building graph schema")
				schema, err := gdb.BuildSchema(ctx, name, server.conf.Server.SchemaInspectN, server.conf.Server.SchemaRandomSample)
				if err == nil {
					log.WithFields(log.Fields{"graph": name}).Debug("cached graph schema")
					err := server.addFullGraph(ctx, fmt.Sprintf(schema.Graph, schemaSuffix), schema)
					if err != nil {
						log.WithFields(log.Fields{"graph": name, "error": err}).Error("failed to store graph schema")
					}
					server.schemas[name] = schema
				} else {
					log.WithFields(log.Fields{"graph": name, "error": err}).Error("failed to build graph schema")
				}
			}
		}
	}
}

// cacheSchemas calls GetSchema on each graph and caches the schemas in memory
func (server *GripServer) cacheSchemas(ctx context.Context) {

	if time.Duration(server.conf.Server.SchemaRefreshInterval) == 0 {
		server.buildSchemas(ctx)
		return
	}

	ticker := time.NewTicker(time.Duration(server.conf.Server.SchemaRefreshInterval))
	server.buildSchemas(ctx)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			server.buildSchemas(ctx)
		}
	}
}

func (server *GripServer) updateGraphMap() {
	o := map[string]string{}
	for k, v := range server.conf.Graphs {
		o[k] = v
	}
	for n, dbs := range server.dbs {
		for _, g := range dbs.ListGraphs() {
			o[g] = n
			if strings.HasSuffix(g, "__mapping__") {
				graph, err := server.getGraph(g)
				if err == nil {
					log.Infof("Reading config for a gripper driver %s", g)
					mapping, _ := gripper.GraphToConfig(graph)
					graphName := strings.TrimSuffix(g, mappingSuffix)
					gdb, err := StartDriver(server.conf, config.DriverConfig{Gripper: &gripper.Config{Graph: graphName, Mapping: mapping}})
					if err == nil {
						driverName := fmt.Sprintf("%s__driver__", graphName)
						server.dbs[driverName] = gdb
						o[graphName] = driverName
					} else {
						log.Errorf("Failed to start gripper: %s", graphName)
					}
				} else {
					log.Errorf("Failed to get graph mapping: %s", err)
				}
			}
		}
	}
	server.graphMap = o
}

func (server *GripServer) addFullGraph(ctx context.Context, graphName string, schema *gripql.Graph) error {
	if graphName == "" {
		return fmt.Errorf("graph name is an empty string")
	}
	if server.graphExists(graphName) {
		_, err := server.DeleteGraph(ctx, &gripql.GraphID{Graph: graphName})
		if err != nil {
			return fmt.Errorf("failed to remove previous schema: %v", err)
		}
	}
	_, err := server.AddGraph(ctx, &gripql.GraphID{Graph: graphName})
	if err != nil {
		return fmt.Errorf("error creating graph '%s': %v", graphName, err)
	}
	for _, v := range schema.Vertices {
		_, err := server.addVertex(ctx, &gripql.GraphElement{Graph: graphName, Vertex: v})
		if err != nil {
			return fmt.Errorf("error adding vertex to graph '%s': %v", graphName, err)
		}
	}
	for _, e := range schema.Edges {
		_, err := server.addEdge(ctx, &gripql.GraphElement{Graph: graphName, Edge: e})
		if err != nil {
			return fmt.Errorf("error adding edge to graph '%s': %v", graphName, err)
		}
	}
	return nil
}
