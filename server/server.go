// Package server contains code for serving the Grip API.
package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/graphql"
	"github.com/bmeg/grip/gripql"
	"github.com/golang/gddo/httputil"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// GripServer is a GRPC based grip server
type GripServer struct {
	db      gdbi.GraphDB
	conf    Config
	schemas map[string]*gripql.GraphSchema
}

// NewGripServer initializes a GRPC server to connect to the graph store
func NewGripServer(db gdbi.GraphDB, conf Config) (*GripServer, error) {
	_, err := os.Stat(conf.WorkDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(conf.WorkDir, 0700)
		if err != nil {
			return nil, fmt.Errorf("creating work dir: %v", err)
		}
	}
	schemas := make(map[string]*gripql.GraphSchema)
	return &GripServer{db: db, conf: conf, schemas: schemas}, nil
}

// handleError is the grpc gateway error handler
func handleError(w http.ResponseWriter, req *http.Request, err string, code int) {
	log.Println("HTTP handler error:", req.URL, err)
	http.Error(w, err, code)
}

// unaryErrorInterceptor is an interceptor function that logs all errors
func unaryErrorInterceptor() grpc.UnaryServerInterceptor {
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

// streamErrorInterceptor is an interceptor function that logs all errors
func streamErrorInterceptor() grpc.StreamServerInterceptor {
	// Return a function that is the interceptor.
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo,
		handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		if err != nil {
			log.Println(info.FullMethod, "failed;", "error:", err)
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
			),
		),
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				streamAuthInterceptor(server.conf.BasicAuth),
				streamErrorInterceptor(),
			),
		),
		grpc.MaxSendMsgSize(1024*1028*16),
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
	mux := http.NewServeMux()
	grpcMux := runtime.NewServeMux(runtime.WithMarshalerOption("*/*", &marsh))
	runtime.OtherErrorHandler = handleError

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

	dashmux := http.NewServeMux()
	httpDir := http.Dir(server.conf.ContentDir)
	dashfs := http.FileServer(httpDir)
	dashmux.HandleFunc("/", func(resp http.ResponseWriter, req *http.Request) {
		file, err := httpDir.Open("index.html")
		if err != nil {
			panic(err)
		}
		io.Copy(resp, file)
	})
	mux.Handle("/static/", dashfs)
	mux.Handle("/favicon.ico", dashfs)

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

		switch httputil.NegotiateContentType(req, []string{"text/*", "text/html"}, "text/*") {
		case "text/html":
			dashmux.ServeHTTP(resp, req)

		default:
			if server.conf.DisableHTTPCache {
				resp.Header().Set("Cache-Control", "no-store")
			}
			grpcMux.ServeHTTP(resp, req)
		}
	})

	// Regsiter Query Service
	gripql.RegisterQueryServer(grpcServer, server)
	err = gripql.RegisterQueryHandlerFromEndpoint(ctx, grpcMux, ":"+server.conf.RPCPort, opts)
	if err != nil {
		return fmt.Errorf("registering query endpoint: %v", err)
	}

	// Regsiter Edit Service
	if !server.conf.ReadOnly {
		gripql.RegisterEditServer(grpcServer, server)
		err = gripql.RegisterEditHandlerFromEndpoint(ctx, grpcMux, ":"+server.conf.RPCPort, opts)
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

	go func() {
		server.cacheSchemas(ctx)
	}()

	log.Println("TCP+RPC server listening on " + server.conf.RPCPort)
	log.Println("HTTP proxy connecting to localhost:" + server.conf.HTTPPort)

	<-ctx.Done()
	log.Println("closing database...")
	if server.db != nil {
		err = server.db.Close()
		if err != nil {
			log.Println("error:", err)
		}
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
