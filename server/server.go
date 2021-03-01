// Package server contains code for serving the Grip API.
package server

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/graphql"
	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jobstorage"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/rpc"
	"github.com/felixge/httpsnoop"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/bmeg/grip/elastic"
	"github.com/bmeg/grip/mongo"
	"github.com/bmeg/grip/grids"
	"github.com/bmeg/grip/gripper"
	"github.com/bmeg/grip/kvgraph"
	esql "github.com/bmeg/grip/existing-sql"
	"github.com/bmeg/grip/psql"
	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/grip/kvi/boltdb"   // import so bolt will register itself
	_ "github.com/bmeg/grip/kvi/leveldb"  // import so level will register itself

)

// GripServer is a GRPC based grip server
type GripServer struct {
	gripql.UnimplementedQueryServer
	gripql.UnimplementedEditServer
	gripql.UnimplementedJobServer
	dbs     map[string]gdbi.GraphDB
	graphMap map[string]string
	conf    *config.Config
	schemas map[string]*gripql.Graph
	baseDir string
	jStorage jobstorage.JobStorage
}

// NewGripServer initializes a GRPC server to connect to the graph store
func NewGripServer(conf *config.Config, schemas map[string]*gripql.Graph, baseDir string) (*GripServer, error) {
	_, err := os.Stat(conf.Server.WorkDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(conf.Server.WorkDir, 0700)
		if err != nil {
			return nil, fmt.Errorf("creating work dir: %v", err)
		}
	}
	if schemas == nil {
		schemas = make(map[string]*gripql.Graph)
	}

	gdbs := map[string]gdbi.GraphDB{}
	for name, dConfig := range conf.Drivers {
		g, err := StartDriver(dConfig, baseDir)
		if err == nil {
			gdbs[name] = g
		}
	}

	server := &GripServer{dbs:gdbs, conf: conf, schemas: schemas}
	for graph, schema := range schemas {
		if !server.graphExists(graph) {
			_, err := server.AddGraph(context.Background(), &gripql.GraphID{Graph: graph})
			if err != nil {
				return nil, fmt.Errorf("error creating graph defined by schema '%s': %v", graph, err)
			}
		}
		err = server.addSchemaGraph(context.Background(), schema)
		if err != nil {
			return nil, err
		}
	}
	server.updateGraphMap()
	return server, nil
}

// StartDriver: based on string entry in config file, figure out which driver to initialize
func StartDriver(d config.DriverConfig, baseDir string) (gdbi.GraphDB, error) {
	if d.Bolt != nil {
		return kvgraph.NewKVGraphDB("bolt", *d.Bolt)
	} else if d.Badger != nil {
		return kvgraph.NewKVGraphDB("badger", *d.Badger)
	} else if d.Level != nil {
		return kvgraph.NewKVGraphDB("level", *d.Level)
	} else if d.Grids != nil {
		return grids.NewGraphDB(*d.Grids)
	} else if d.Elasticsearch != nil {
		return elastic.NewGraphDB(*d.Elasticsearch)
	} else if d.MongoDB != nil {
		return mongo.NewGraphDB(*d.MongoDB)
	} else if d.PSQL != nil {
		return psql.NewGraphDB(*d.PSQL)
	} else if d.ExistingSQL != nil {
		return esql.NewGraphDB(*d.ExistingSQL)
	} else if d.Gripper != nil {
		return gripper.NewGDB(*d.Gripper, baseDir)
	}
	return nil, fmt.Errorf("unknown driver: %#v", d)
}


func (server *GripServer) updateGraphMap() {
	o := map[string]string{}
	for k, v := range server.conf.Graphs {
		o[k] = v
	}
	for n, dbs := range server.dbs {
		for _, g := range dbs.ListGraphs() {
			o[g] = n
		}
	}
	server.graphMap = o
}


func (server *GripServer) getGraphDB(graph string) (gdbi.GraphDB, error) {
	if driverName, ok := server.graphMap[graph]; ok {
		if gdb, ok := server.dbs[driverName]; ok {
			return gdb, nil
		}
	} else {
		if gdb, ok := server.dbs[server.conf.Default]; ok {
			return gdb, nil
		}
	}
	return nil, fmt.Errorf("Driver not found")
}

// Serve starts the server and does not block. This will open TCP ports
// for both RPC and HTTP.
func (server *GripServer) Serve(pctx context.Context) error {
	ctx, cancel := context.WithCancel(pctx)
	defer cancel()

	lis, err := net.Listen("tcp", ":"+server.conf.Server.RPCPort)
	if err != nil {
		return fmt.Errorf("Cannot open port: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				unaryAuthInterceptor(server.conf.Server.BasicAuth),
				unaryInterceptor(server.conf.Server.RequestLogging.Enable, server.conf.Server.RequestLogging.HeaderWhitelist),
			),
		),
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				streamAuthInterceptor(server.conf.Server.BasicAuth),
				streamInterceptor(server.conf.Server.RequestLogging.Enable, server.conf.Server.RequestLogging.HeaderWhitelist),
			),
		),
		grpc.MaxSendMsgSize(1024*1024*16),
		grpc.MaxRecvMsgSize(1024*1024*16),
	)

	// Setup RESTful proxy
	marsh := MarshalClean{
		m: &runtime.JSONPb{
			protojson.MarshalOptions{EmitUnpopulated: true},
			protojson.UnmarshalOptions{},
			//EnumsAsInts:  false,
			//EmitDefaults: true,
			//OrigName:     true,
		},
	}
	grpcMux := runtime.NewServeMux(runtime.WithMarshalerOption("*/*", &marsh))
	mux := http.NewServeMux()

	// Setup GraphQL handler
	user := ""
	password := ""
	if len(server.conf.Server.BasicAuth) > 0 {
		user = server.conf.Server.BasicAuth[0].User
		password = server.conf.Server.BasicAuth[0].Password
	}
	gqlHandler, err := graphql.NewHTTPHandler(server.conf.Server.RPCAddress(), user, password)
	if err != nil {
		return fmt.Errorf("setting up GraphQL handler: %v", err)
	}
	mux.Handle("/graphql/", gqlHandler)

	// Setup web ui handler
	dashmux := http.NewServeMux()
	if server.conf.Server.ContentDir != "" {
		httpDir := http.Dir(server.conf.Server.ContentDir)
		dashfs := http.FileServer(httpDir)
		dashmux.Handle("/", dashfs)
	}

	// Setup logic to route to either HTML site or API
	// HTTP middleware is injected here as well
	mux.HandleFunc("/", func(resp http.ResponseWriter, req *http.Request) {
		start := time.Now()

		if len(server.conf.Server.BasicAuth) > 0 {
			resp.Header().Set("WWW-Authenticate", "Basic")
			u, p, ok := req.BasicAuth()
			if !ok {
				http.Error(resp, "authorization failed", http.StatusUnauthorized)
				return
			}
			authorized := false
			for _, cred := range server.conf.Server.BasicAuth {
				if cred.User == u && cred.Password == p {
					authorized = true
				}
			}
			if !authorized {
				http.Error(resp, "permission denied", http.StatusForbidden)
				return
			}
		}

		switch strings.HasPrefix(req.URL.Path, "/v1/") {
		case true:
			if server.conf.Server.DisableHTTPCache {
				resp.Header().Set("Cache-Control", "no-store")
			}

			// copy body and return it to request
			var body []byte
			if server.conf.Server.RequestLogging.Enable {
				body, _ = ioutil.ReadAll(req.Body)
				req.Body = ioutil.NopCloser(bytes.NewBuffer(body))
			}

			// handle the request
			m := httpsnoop.CaptureMetrics(grpcMux, resp, req)

			if !server.conf.Server.RequestLogging.Enable {
				return
			}

			// copy whitelisted headers for logging
			headers := extractHeaderKeys(req.Header, server.conf.Server.RequestLogging.HeaderWhitelist)

			// log the request
			entry := log.WithFields(log.Fields{
				"path":    req.URL.Path,
				"request": string(body),
				"header":  headers,
				"latency": time.Since(start).String(),
				"status":  m.Code,
			})
			if m.Code == http.StatusOK {
				entry.Info("HTTP server responded")
			} else {
				entry.Error("HTTP server responded")
			}

		case false:
			dashmux.ServeHTTP(resp, req)
		}
	})

	// Regsiter Query Service
	gripql.RegisterQueryServer(grpcServer, server)
	//TODO: Put in some sort of logic that will allow web server to be configured to use GRPC client
	err = gripql.RegisterQueryHandlerClient(ctx, grpcMux, gripql.NewQueryDirectClient(server))
	//err = gripql.RegisterQueryHandlerFromEndpoint(ctx, grpcMux, ":"+server.conf.RPCPort, []grpc.DialOption{grpc.WithInsecure()})
	if err != nil {
		return fmt.Errorf("registering query endpoint: %v", err)
	}

	// Regsiter Edit Service
	if !server.conf.Server.ReadOnly {
		gripql.RegisterEditServer(grpcServer, server)
		//TODO: Put in some sort of logic that will allow web server to be configured to use GRPC client
		err = gripql.RegisterEditHandlerClient(ctx, grpcMux, gripql.NewEditDirectClient(server))
		//err = gripql.RegisterEditHandlerFromEndpoint(ctx, grpcMux, ":"+server.conf.RPCPort, []grpc.DialOption{grpc.WithInsecure()})
		if err != nil {
			return fmt.Errorf("registering edit endpoint: %v", err)
		}
	}
	
	if !server.conf.Server.NoJobs {
		gripql.RegisterJobServer(grpcServer, server)
		//TODO: Put in some sort of logic that will allow web server to be configured to use GRPC client
		err = gripql.RegisterJobHandlerClient(ctx, grpcMux, gripql.NewJobDirectClient(server))
		if err != nil {
			return fmt.Errorf("registering job endpoint: %v", err)
		}		
	}

	httpServer := &http.Server{
		Addr:    ":" + server.conf.Server.HTTPPort,
		Handler: mux,
	}

	var grpcErr error
	var httpErr error
	go func() {
		grpcErr = grpcServer.Serve(lis)
		cancel()
	}()

	go func() {
		httpErr = httpServer.ListenAndServe()
		cancel()
	}()

	log.Infoln("TCP+RPC server listening on " + server.conf.Server.RPCPort)
	log.Infoln("HTTP proxy connecting to localhost:" + server.conf.Server.HTTPPort)

	// load existing schemas from db
	for _, gdb := range server.dbs {
		for _, graph := range gdb.ListGraphs() {
			if isSchema(graph) {
				log.WithFields(log.Fields{"graph": graph}).Debug("Loading existing schema into cache")
				conn, err := gripql.Connect(rpc.ConfigWithDefaults(server.conf.Server.RPCAddress()), true)
				if err != nil {
					return fmt.Errorf("failed to load existing schema: %v", err)
				}
				res, err := conn.Traversal(&gripql.GraphQuery{Graph: graph, Query: gripql.NewQuery().V().Statements})
				if err != nil {
					return fmt.Errorf("failed to load existing schema: %v", err)
				}
				vertices := []*gripql.Vertex{}
				for row := range res {
					vertices = append(vertices, row.GetVertex())
				}
				res, err = conn.Traversal(&gripql.GraphQuery{Graph: graph, Query: gripql.NewQuery().E().Statements})
				if err != nil {
					return fmt.Errorf("failed to load existing schema: %v", err)
				}
				edges := []*gripql.Edge{}
				for row := range res {
					edges = append(edges, row.GetEdge())
				}
				graph = strings.TrimSuffix(graph, schemaSuffix)
				server.schemas[graph] = &gripql.Graph{Graph: graph, Vertices: vertices, Edges: edges}
			}
		}
	}

	if server.conf.Server.AutoBuildSchemas {
		go func() {
			server.cacheSchemas(ctx)
		}()
	}

	<-ctx.Done() //This will hold until canceled, usually from kill signal
	log.Infoln("closing database...")
	for _, gdb := range server.dbs {
		err = gdb.Close()
		if err != nil {
			log.Errorln("db.Close() error:", err)
		}
	}
	log.Infoln("shutting down RPC server...")
	grpcServer.GracefulStop()
	log.Infoln("shutting down HTTP proxy...")
	err = httpServer.Shutdown(context.TODO())
	if err != nil {
		log.Errorf("shutdown error: %v", err)
	}

	if grpcErr != nil || httpErr != nil {
		return fmt.Errorf("gRPC Server Error: %v\nHTTP Server Error: %v", grpcErr, httpErr)
	}
	return nil
}
