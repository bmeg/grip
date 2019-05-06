// Package server contains code for serving the Grip API.
package server

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// GripServer is a GRPC based grip server
type GripServer struct {
	db      gdbi.GraphDB
	conf    Config
	schemas map[string]*gripql.Graph
	gql     *GraphQLHandler
}

// NewGripServer initializes a GRPC server to connect to the graph store
func NewGripServer(db gdbi.GraphDB, conf Config, schemas map[string]*gripql.Graph) (*GripServer, error) {
	if schemas == nil {
		schemas = make(map[string]*gripql.Graph)
	}
	server := &GripServer{db: db, conf: conf, schemas: schemas}
	for graph, schema := range schemas {
		if !engine.GraphExists(db, graph) {
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
	_, err := os.Stat(conf.WorkDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(conf.WorkDir, 0700)
		if err != nil {
			return nil, fmt.Errorf("creating work dir: %v", err)
		}
	}
	server.gql = NewGraphQLHandler(db, conf.WorkDir)
	return server, nil
}

// handleError is the grpc gateway error handler
func handleError(w http.ResponseWriter, req *http.Request, err string, code int) {
	log.WithFields(log.Fields{"url": req.URL, "error": err}).Error("HTTP handler error")
	http.Error(w, err, code)
}

// Return a new interceptor function that logs all requests at the Debug level
func unaryDebugInterceptor() grpc.UnaryServerInterceptor {
	// Return a function that is the interceptor.
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		log.WithFields(log.Fields{
			"endpoint":     info.FullMethod,
			"request":      req,
			"elapsed_time": time.Since(start),
			"error":        err}).Debug("Responding to request")
		return resp, err
	}
}

// Return a new interceptor function that logs all requests at the Debug level
func streamDebugInterceptor() grpc.StreamServerInterceptor {
	// Return a function that is the interceptor.
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo,
		handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		log.WithFields(log.Fields{
			"endpoint":     info.FullMethod,
			"elapsed_time": time.Since(start),
			"error":        err}).Debug("Responding to request")
		return err
	}
}

// unaryErrorInterceptor is an interceptor function that logs all errors
func unaryErrorInterceptor() grpc.UnaryServerInterceptor {
	// Return a function that is the interceptor.
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			log.WithFields(log.Fields{"endpoint": info.FullMethod, "request": req, "error": err}).Error("Request failed")
		}
		return resp, err
	}
}

// streamErrorInterceptor is an interceptor function that logs all errors
func streamErrorInterceptor() grpc.StreamServerInterceptor {
	// Return a function that is the interceptor.
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo,
		handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		if err != nil {
			log.WithFields(log.Fields{"endpoint": info.FullMethod, "error": err}).Error("Request failed")
		}
		return err
	}
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
				unaryErrorInterceptor(),
				unaryDebugInterceptor(),
			),
		),
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				streamAuthInterceptor(server.conf.BasicAuth),
				streamErrorInterceptor(),
				streamDebugInterceptor(),
			),
		),
		grpc.MaxSendMsgSize(1024*1024*16),
		grpc.MaxRecvMsgSize(1024*1024*16),
	)

	//setup RESTful proxy
	marsh := MarshalClean{
		m: &runtime.JSONPb{
			EnumsAsInts:  false,
			EmitDefaults: true,
			OrigName:     true,
		},
	}
	grpcMux := runtime.NewServeMux(runtime.WithMarshalerOption("*/*", &marsh))
	runtime.OtherErrorHandler = handleError

	mux := http.NewServeMux()
	mux.Handle("/graphql/", gqlHandler)
	go func() {
		gqlHandler.BuildAllGraphHandlers(ctx)
	}()

	dashmux := http.NewServeMux()
	if server.conf.ContentDir != "" {
		httpDir := http.Dir(server.conf.ContentDir)
		dashfs := http.FileServer(httpDir)
		dashmux.Handle("/", dashfs)
	}

	mux.HandleFunc("/", func(resp http.ResponseWriter, req *http.Request) {

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
			grpcMux.ServeHTTP(resp, req)

		case false:
			dashmux.ServeHTTP(resp, req)
		}
	})

	// Regsiter Query Service
	gripql.RegisterQueryServer(grpcServer, server)
	// TODO: Put in some sort of logic that will allow web server to be configured to use GRPC client
	err = gripql.RegisterQueryHandlerClient(ctx, grpcMux, gripql.NewQueryDirectClient(server))
	// err = gripql.RegisterQueryHandlerFromEndpoint(ctx, grpcMux, ":"+server.conf.RPCPort, opts)
	if err != nil {
		return fmt.Errorf("registering query endpoint: %v", err)
	}

	// Regsiter Edit Service
	if !server.conf.ReadOnly {
		gripql.RegisterEditServer(grpcServer, server)
		// TODO: Put in some sort of logic that will allow web server to be configured to use GRPC client
		err = gripql.RegisterEditHandlerClient(ctx, grpcMux, gripql.NewEditDirectClient(server))
		// err = gripql.RegisterEditHandlerFromEndpoint(ctx, grpcMux, ":"+server.conf.RPCPort, opts)
		if err != nil {
			return fmt.Errorf("registering edit endpoint: %v", err)
		}
	}

	httpServer := &http.Server{
		Addr:    ":" + server.conf.HTTPPort,
		Handler: mux,
	}

	var srverr error
	go func() {
		srverr = grpcServer.Serve(lis)
		cancel()
	}()

	go func() {
		srverr = httpServer.ListenAndServe()
		cancel()
	}()

	log.Infoln("TCP+RPC server listening on " + server.conf.RPCPort)
	log.Infoln("HTTP proxy connecting to localhost:" + server.conf.HTTPPort)

	// load existing schemas from db
	if server.db != nil {
		for _, graph := range server.db.ListGraphs() {
			if gripql.IsSchema(graph) {
				log.WithFields(log.Fields{"graph": graph}).Debug("Loading existing schema into cache")
				s, err := engine.GetSchema(ctx, server.db, server.conf.WorkDir, graph)
				if err != nil {
					return fmt.Errorf("failed to load existing schema: %v", err)
				}
				server.schemas[graph] = s
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
			log.Println("error:", err)
		}
	}
	log.Infoln("shutting down RPC server...")
	grpcServer.GracefulStop()
	log.Infoln("shutting down HTTP proxy...")
	err = httpServer.Shutdown(context.TODO())
	if err != nil {
		log.Errorf("shutdown error: %v", err)
	}

	return srverr
}
