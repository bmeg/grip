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
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/rpc"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// GripServer is a GRPC based grip server
type GripServer struct {
	db      gdbi.GraphDB
	conf    Config
	schemas map[string]*gripql.Graph
}

// NewGripServer initializes a GRPC server to connect to the graph store
func NewGripServer(db gdbi.GraphDB, conf Config, schemas map[string]*gripql.Graph) (*GripServer, error) {
	_, err := os.Stat(conf.WorkDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(conf.WorkDir, 0700)
		if err != nil {
			return nil, fmt.Errorf("creating work dir: %v", err)
		}
	}
	if schemas == nil {
		schemas = make(map[string]*gripql.Graph)
	}
	server := &GripServer{db: db, conf: conf, schemas: schemas}
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
	return server, nil
}

// Serve starts the server and does not block. This will open TCP ports
// for both RPC and HTTP.
func (server *GripServer) Serve(pctx context.Context) error {
	ctx, cancel := context.WithCancel(pctx)
	defer cancel()

	lis, err := net.Listen("tcp", ":"+server.conf.RPCPort)
	if err != nil {
		return fmt.Errorf("Cannot open port: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				unaryAuthInterceptor(server.conf.BasicAuth),
				unaryInterceptor(server.conf.RequestLogging.Enable, server.conf.RequestLogging.HeaderWhitelist),
			),
		),
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				streamAuthInterceptor(server.conf.BasicAuth),
				streamInterceptor(server.conf.RequestLogging.Enable, server.conf.RequestLogging.HeaderWhitelist),
			),
		),
		grpc.MaxSendMsgSize(1024*1024*16),
		grpc.MaxRecvMsgSize(1024*1024*16),
	)

	// Setup RESTful proxy
	marsh := MarshalClean{
		m: &runtime.JSONPb{
			EnumsAsInts:  false,
			EmitDefaults: true,
			OrigName:     true,
		},
	}
	grpcMux := runtime.NewServeMux(runtime.WithMarshalerOption("*/*", &marsh))
	mux := http.NewServeMux()

	// Setup GraphQL handler
	user := ""
	password := ""
	if len(server.conf.BasicAuth) > 0 {
		user = server.conf.BasicAuth[0].User
		password = server.conf.BasicAuth[0].Password
	}
	gqlHandler, err := graphql.NewHTTPHandler(server.conf.RPCAddress(), user, password)
	if err != nil {
		return fmt.Errorf("setting up GraphQL handler: %v", err)
	}
	mux.Handle("/graphql/", gqlHandler)

	// Setup web ui handler
	dashmux := http.NewServeMux()
	if server.conf.ContentDir != "" {
		httpDir := http.Dir(server.conf.ContentDir)
		dashfs := http.FileServer(httpDir)
		dashmux.Handle("/", dashfs)
	}

	// Setup logic to route to either HTML site or API
	// HTTP middleware is injected here as well
	mux.HandleFunc("/", func(resp http.ResponseWriter, req *http.Request) {
		start := time.Now()

		if len(server.conf.BasicAuth) > 0 {
			resp.Header().Set("WWW-Authenticate", "Basic")
			u, p, ok := req.BasicAuth()
			if !ok {
				http.Error(resp, "authorization failed", http.StatusUnauthorized)
				return
			}
			authorized := false
			for _, cred := range server.conf.BasicAuth {
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
			if server.conf.DisableHTTPCache {
				resp.Header().Set("Cache-Control", "no-store")
			}

			// copy body and return it to request
			var body []byte
			if server.conf.RequestLogging.Enable {
				body, _ = ioutil.ReadAll(req.Body)
				req.Body = ioutil.NopCloser(bytes.NewBuffer(body))
			}

			// handle the request
			lrw := &loggingResponseWriter{resp, http.StatusOK}
			grpcMux.ServeHTTP(lrw, req)

			if !server.conf.RequestLogging.Enable {
				return
			}

			// copy whitelisted headers for logging
			headers := extractHeaderKeys(req.Header, server.conf.RequestLogging.HeaderWhitelist)

			// log the request
			entry := log.WithFields(log.Fields{
				"path":    req.URL.Path,
				"request": string(body),
				"header":  headers,
				"latency": time.Since(start).String(),
				"status":  lrw.statusCode,
			})
			if lrw.statusCode == http.StatusOK {
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
	//err = gripql.RegisterQueryHandlerFromEndpoint(ctx, grpcMux, ":"+server.conf.RPCPort, opts)
	if err != nil {
		return fmt.Errorf("registering query endpoint: %v", err)
	}

	// Regsiter Edit Service
	if !server.conf.ReadOnly {
		gripql.RegisterEditServer(grpcServer, server)
		//TODO: Put in some sort of logic that will allow web server to be configured to use GRPC client
		err = gripql.RegisterEditHandlerClient(ctx, grpcMux, gripql.NewEditDirectClient(server))
		//err = gripql.RegisterEditHandlerFromEndpoint(ctx, grpcMux, ":"+server.conf.RPCPort, opts)
		if err != nil {
			return fmt.Errorf("registering edit endpoint: %v", err)
		}
	}

	httpServer := &http.Server{
		Addr:    ":" + server.conf.HTTPPort,
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

	log.Infoln("TCP+RPC server listening on " + server.conf.RPCPort)
	log.Infoln("HTTP proxy connecting to localhost:" + server.conf.HTTPPort)

	// load existing schemas from db
	if server.db != nil {
		for _, graph := range server.db.ListGraphs() {
			if isSchema(graph) {
				log.WithFields(log.Fields{"graph": graph}).Debug("Loading existing schema into cache")
				conn, err := gripql.Connect(rpc.ConfigWithDefaults(server.conf.RPCAddress()), true)
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

	if server.conf.AutoBuildSchemas {
		go func() {
			server.cacheSchemas(ctx)
		}()
	}

	<-ctx.Done()
	log.Infoln("closing database...")
	if server.db != nil {
		err = server.db.Close()
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
