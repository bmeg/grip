// Package server contains code for serving the Arachne API.
package server

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/graphql"
	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// ArachneServer is a GRPC based arachne server
type ArachneServer struct {
	db   gdbi.GraphDB
	conf Config
}

// NewArachneServer initializes a GRPC server to connect to the graph store
func NewArachneServer(db gdbi.GraphDB, conf Config) (*ArachneServer, error) {
	_, err := os.Stat(conf.WorkDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(conf.WorkDir, 0700)
		if err != nil {
			return nil, fmt.Errorf("creating work dir:", err)
		}
	}

	return &ArachneServer{db: db, conf: conf}, nil
}

// handleError is the grpc gateway error handler
func handleError(w http.ResponseWriter, req *http.Request, err string, code int) {
	log.Println("HTTP handler error:", req.URL, ";", "error", err)
	http.Error(w, err, code)
}

// errorInterceptor is an interceptor function that logs all errors
func errorInterceptor() grpc.UnaryServerInterceptor {
	// Return a function that is the interceptor.
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			log.Println(info.FullMethod, "failed;", "error:", err)
		}
		return resp, err
	}
}

// Serve starts the server and does not block. This will open TCP ports
// for both RPC and HTTP.
func (server *ArachneServer) Serve(pctx context.Context) error {
	ctx, cancel := context.WithCancel(pctx)
	defer cancel()

	lis, err := net.Listen("tcp", ":"+server.conf.RPCPort)
	if err != nil {
		return fmt.Errorf("Cannot open port: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				newAuthInterceptor(server.conf.BasicAuth),
				errorInterceptor(),
			),
		),
	)
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	//setup RESTful proxy
	marsh := MarshalClean{
		m: &runtime.JSONPb{
			EnumsAsInts:  false,
			EmitDefaults: true,
			OrigName:     true,
		},
	}
	grpcMux := runtime.NewServeMux(runtime.WithMarshalerOption("*", &marsh))
	runtime.OtherErrorHandler = handleError

	r := mux.NewRouter()
	r.PathPrefix("/graphql/").Handler(graphql.NewHTTPHandler(":" + server.conf.RPCPort))
	r.PathPrefix("/v1/").Handler(grpcMux)
	if server.conf.ContentDir != "" {
		r.PathPrefix("/").Handler(http.StripPrefix("/", http.FileServer(http.Dir(server.conf.ContentDir))))
	}
	r.HandleFunc("/", func(resp http.ResponseWriter, req *http.Request) {
		if len(server.conf.BasicAuth) > 0 {
			resp.Header().Set("WWW-Authenticate", "Basic")
		}
		if server.conf.DisableHTTPCache {
			resp.Header().Set("Cache-Control", "no-store")
		}
		grpcMux.ServeHTTP(resp, req)
	})

	// Regsiter Query Service
	aql.RegisterQueryServer(grpcServer, server)
	err = aql.RegisterQueryHandlerFromEndpoint(ctx, grpcMux, ":"+server.conf.RPCPort, opts)
	if err != nil {
		return fmt.Errorf("registering query endpoint: %v", err)
	}

	// Regsiter Edit Service
	if !server.conf.ReadOnly {
		aql.RegisterEditServer(grpcServer, server)
		err = aql.RegisterEditHandlerFromEndpoint(ctx, grpcMux, ":"+server.conf.RPCPort, opts)
		if err != nil {
			return fmt.Errorf("registering edit endpoint: %v", err)
		}
	}

	httpServer := &http.Server{
		Addr:    ":" + server.conf.HTTPPort,
		Handler: r,
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

	log.Println("TCP+RPC server listening on " + server.conf.RPCPort)
	log.Println("HTTP proxy connecting to localhost:" + server.conf.HTTPPort)

	<-ctx.Done()
	log.Println("closing database...")
	err = server.db.Close()
	if err != nil {
		log.Println("error:", err)
	}
	log.Println("shutting down RPC server...")
	grpcServer.GracefulStop()
	log.Println("shutting down HTTP proxy...")
	err = httpServer.Shutdown(context.TODO())
	if err != nil {
		log.Println("error:", err)
	}

	return srverr
}
