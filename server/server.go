// Package server contains code for serving the Grip API.
package server

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bmeg/grip/config"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jobstorage"
	"github.com/bmeg/grip/log"
	"github.com/felixge/httpsnoop"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/bmeg/grip/elastic"
	esql "github.com/bmeg/grip/existing-sql"
	"github.com/bmeg/grip/grids"
	"github.com/bmeg/grip/gripper"
	"github.com/bmeg/grip/kvgraph"
	_ "github.com/bmeg/grip/kvi/badgerdb" // import so badger will register itself
	_ "github.com/bmeg/grip/kvi/boltdb"   // import so bolt will register itself
	_ "github.com/bmeg/grip/kvi/leveldb"  // import so level will register itself
	_ "github.com/bmeg/grip/kvi/pebbledb" // import so level will register itself
	"github.com/bmeg/grip/mongo"
	"github.com/bmeg/grip/psql"
)

// GripServer is a GRPC based grip server
type GripServer struct {
	gripql.UnimplementedQueryServer
	gripql.UnimplementedEditServer
	gripql.UnimplementedJobServer
	gripql.UnimplementedConfigureServer
	dbs      map[string]gdbi.GraphDB  //graph database drivers
	graphMap map[string]string        //mapping from graph name to graph database driver
	conf     *config.Config           //global configuration
	schemas  map[string]*gripql.Graph //cached schemas
	mappings map[string]*gripql.Graph //cached gripper graph mappings
	plugins  map[string]*Plugin
	sources  map[string]gripper.GRIPSourceClient
	baseDir  string
	jStorage jobstorage.JobStorage
}

// NewGripServer initializes a GRPC server to connect to the graph store
func NewGripServer(conf *config.Config, baseDir string, drivers map[string]gdbi.GraphDB) (*GripServer, error) {
	_, err := os.Stat(conf.Server.WorkDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(conf.Server.WorkDir, 0700)
		if err != nil {
			return nil, fmt.Errorf("creating work dir: %v", err)
		}
	}
	schemas := make(map[string]*gripql.Graph)

	gdbs := map[string]gdbi.GraphDB{}
	if drivers != nil {
		for i, d := range drivers {
			gdbs[i] = d
		}
	}

	sources := map[string]gripper.GRIPSourceClient{}
	for name, host := range conf.Sources {
		conn, err := gripper.StartConnection(host)
		if err == nil {
			sources[name] = conn
		} else {
			log.Errorf("Cannot reach source: %s", name)
		}
	}

	for name, dConfig := range conf.Drivers {
		if _, ok := gdbs[name]; !ok {
			g, err := StartDriver(dConfig, sources)
			if err == nil {
				gdbs[name] = g
			} else {
				log.Errorf("Driver start error: %s", err)
			}
		}
	}

	server := &GripServer{
		dbs:      gdbs,
		conf:     conf,
		schemas:  schemas,
		mappings: map[string]*gripql.Graph{},
		plugins:  map[string]*Plugin{},
		sources:  sources,
	}

	if conf.Default == "" {
		//if no default is found set it to the first driver found
		for i := range gdbs {
			if conf.Default == "" {
				conf.Default = i
			}
		}
	}
	if _, ok := gdbs[conf.Default]; !ok {
		return nil, fmt.Errorf("default driver '%s' does not exist", conf.Default)
	}
	fmt.Printf("Default graph driver: %s\n", conf.Default)
	return server, nil
}

// StartDriver: based on string entry in config file, figure out which driver to initialize
func StartDriver(d config.DriverConfig, sources map[string]gripper.GRIPSourceClient) (gdbi.GraphDB, error) {
	if d.Bolt != nil {
		return kvgraph.NewKVGraphDB("bolt", *d.Bolt)
	} else if d.Badger != nil {
		return kvgraph.NewKVGraphDB("badger", *d.Badger)
	} else if d.Level != nil {
		return kvgraph.NewKVGraphDB("level", *d.Level)
	} else if d.Pebble != nil {
		return kvgraph.NewKVGraphDB("pebble", *d.Pebble)
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
		return gripper.NewGDBFromConfig(d.Gripper.Graph, d.Gripper.Mapping, sources)
	}
	return nil, fmt.Errorf("unknown driver: %#v", d)
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
	return nil, fmt.Errorf("driver not found")
}

// Serve starts the server and does not block. This will open TCP ports
// for both RPC and HTTP.
func (server *GripServer) Serve(pctx context.Context) error {
	ctx, cancel := context.WithCancel(pctx)
	defer cancel()

	lis, err := net.Listen("tcp", ":"+server.conf.Server.RPCPort)
	if err != nil {
		return fmt.Errorf("cannot open port: %v", err)
	}

	unaryAuthInt := server.conf.Server.Accounts.UnaryInterceptor()
	streamAuthInt := server.conf.Server.Accounts.StreamInterceptor()

	chainUnaryInt := grpc.UnaryInterceptor(
		grpc_middleware.ChainUnaryServer(
			unaryAuthInt,
			unaryInterceptor(server.conf.Server.RequestLogging.Enable, server.conf.Server.RequestLogging.HeaderWhitelist),
		),
	)

	chainStreamInt := grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(
			streamAuthInt,
			streamInterceptor(server.conf.Server.RequestLogging.Enable, server.conf.Server.RequestLogging.HeaderWhitelist),
		),
	)

	grpcServer := grpc.NewServer(
		chainUnaryInt,
		chainStreamInt,
		grpc.MaxSendMsgSize(1024*1024*16),
		grpc.MaxRecvMsgSize(1024*1024*16),
	)

	// Setup RESTful proxy
	marsh := NewMarshaler()
	grpcMux := runtime.NewServeMux(runtime.WithMarshalerOption("*/*", marsh))
	mux := http.NewServeMux()

	// Setup GraphQL handler
	/*
		user := ""
		password := ""
		if len(server.conf.Server.BasicAuth) > 0 {
			user = server.conf.Server.BasicAuth[0].User
			password = server.conf.Server.BasicAuth[0].Password
		}
		gqlHandler, err := graphql.NewHTTPHandler(server.conf.Server.RPCAddress(), user, password)
		if err != nil {
			return fmt.Errorf("setting up GraphQL handler: %v", err)
		}*/
	/*
		gqlHandler, err := graphql.NewClientHTTPHandler(
			gripql.WrapClient(gripql.NewQueryDirectClient(
				server,
				gripql.DirectUnaryInterceptor(unaryAuthInt),
				gripql.DirectStreamInterceptor(streamAuthInt),
			),
				nil, nil, nil))

		mux.Handle("/graphql/", gqlHandler)
	*/

	for name, setup := range endpointMap {
		queryClient := gripql.NewQueryDirectClient(
			server,
			gripql.DirectUnaryInterceptor(unaryAuthInt),
			gripql.DirectStreamInterceptor(streamAuthInt),
		)
		// TODO: make writeClient initialization configurable
		writeClient := gripql.NewEditDirectClient(
			server,
			gripql.DirectUnaryInterceptor(unaryAuthInt),
			gripql.DirectStreamInterceptor(streamAuthInt),
		)
		cfg := endpointConfig[name]
		handler, err := setup(gripql.WrapClient(queryClient, writeClient, nil, nil), cfg)
		if err == nil {
			log.Infof("Plugin added to /%s/", name)
			prefix := fmt.Sprintf("/%s/", name)
			mux.Handle(prefix, http.StripPrefix(prefix, handler))
		} else {
			log.Errorf("Unable to load plugin %s: %s", name, err)
		}
	}

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

		/*
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
		*/

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
	err = gripql.RegisterQueryHandlerClient(
		ctx, grpcMux,
		gripql.NewQueryDirectClient(
			server,
			gripql.DirectUnaryInterceptor(unaryAuthInt),
			gripql.DirectStreamInterceptor(streamAuthInt),
		))
	//err = gripql.RegisterQueryHandlerFromEndpoint(ctx, grpcMux, ":"+server.conf.RPCPort, []grpc.DialOption{grpc.WithInsecure()})
	if err != nil {
		return fmt.Errorf("registering query endpoint: %v", err)
	}

	// Regsiter Edit Service
	if !server.conf.Server.ReadOnly {
		gripql.RegisterEditServer(grpcServer, server)
		//TODO: Put in some sort of logic that will allow web server to be configured to use GRPC client
		err = gripql.RegisterEditHandlerClient(
			ctx, grpcMux,
			gripql.NewEditDirectClient(
				server,
				gripql.DirectUnaryInterceptor(unaryAuthInt),
				gripql.DirectStreamInterceptor(streamAuthInt),
			))
		//err = gripql.RegisterEditHandlerFromEndpoint(ctx, grpcMux, ":"+server.conf.RPCPort, []grpc.DialOption{grpc.WithInsecure()})
		if err != nil {
			return fmt.Errorf("registering edit endpoint: %v", err)
		}
	}

	if !server.conf.Server.NoJobs {
		gripql.RegisterJobServer(grpcServer, server)
		err = gripql.RegisterJobHandlerClient(ctx, grpcMux,
			gripql.NewJobDirectClient(
				server,
				gripql.DirectUnaryInterceptor(unaryAuthInt),
				gripql.DirectStreamInterceptor(streamAuthInt),
			))
		if err != nil {
			return fmt.Errorf("registering job endpoint: %v", err)
		}
		jobDir := filepath.Join(server.conf.Server.WorkDir, "jobs")
		server.jStorage = jobstorage.NewFSJobStorage(jobDir)
	}

	if server.conf.Server.EnablePlugins {
		gripql.RegisterConfigureServer(grpcServer, server)
		err = gripql.RegisterConfigureHandlerClient(ctx, grpcMux,
			gripql.NewConfigureDirectClient(
				server,
				gripql.DirectUnaryInterceptor(unaryAuthInt),
				gripql.DirectStreamInterceptor(streamAuthInt),
			))
		if err != nil {
			return fmt.Errorf("registering plugin endpoint: %v", err)
		}
	} else {
		gripql.RegisterConfigureServer(grpcServer, &nullPluginServer{})
		err = gripql.RegisterConfigureHandlerClient(ctx, grpcMux, gripql.NewConfigureDirectClient(&nullPluginServer{}))
		if err != nil {
			return fmt.Errorf("registering plugin endpoint: %v", err)
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
				schema, err := server.getGraph(graph)
				if err == nil {
					server.schemas[strings.TrimSuffix(graph, schemaSuffix)] = schema
				}
			} else if isMapping(graph) {
				log.WithFields(log.Fields{"graph": graph}).Debug("Loading existing mapping into cache")
				mapping, err := server.getGraph(graph)
				if err == nil {
					server.mappings[strings.TrimSuffix(graph, mappingSuffix)] = mapping
				}
			}
		}
	}

	server.updateGraphMap()

	if server.conf.Server.AutoBuildSchemas {
		go func() {
			server.cacheSchemas(ctx)
		}()
	}

	<-ctx.Done() //This will hold until canceled, usually from kill signal
	log.Infoln("shutting down RPC server...")
	grpcServer.GracefulStop()
	log.Infoln("shutting down HTTP proxy...")
	err = httpServer.Shutdown(context.TODO())
	if err != nil {
		log.Errorf("shutdown error: %v", err)
	}

	log.Infoln("closing database...")
	for _, gdb := range server.dbs {
		err = gdb.Close()
		if err != nil {
			log.Errorln("db.Close() error:", err)
		}
	}

	server.ClosePlugins()

	if grpcErr != nil || httpErr != nil {
		return fmt.Errorf("gRPC Server Error: %v\nHTTP Server Error: %v", grpcErr, httpErr)
	}
	return nil
}
