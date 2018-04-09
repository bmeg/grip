package graphserver

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime/debug"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/falcor"
	"github.com/bmeg/arachne/graphql"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// HandleError logs errors and returns http respose codes
func HandleError(w http.ResponseWriter, req *http.Request, error string, code int) {
	fmt.Println(error)
	fmt.Println(req.URL)
	debug.PrintStack()
	http.Error(w, error, code)
}

//MarshalClean is a shim class to 'fix' outgoing streamed messages
//in the default implementation, grpc-gateway wraps the individual messages
//of the stream with a {"result" : <value>}. The cleaner idendifies that and
//removes the wrapper
type MarshalClean struct {
	m runtime.Marshaler
}

// ContentType return content type of marshler
func (mclean *MarshalClean) ContentType() string {
	return mclean.m.ContentType()
}

// Marshal serializes v into a JSON encoded byte array. If v is of
// type `proto.Message` the then field "result" is extracted and returned by
// itself. This is mainly to get around a weird behavior of the GRPC gateway
// streaming output
func (mclean *MarshalClean) Marshal(v interface{}) ([]byte, error) {
	if x, ok := v.(map[string]proto.Message); ok {
		return mclean.m.Marshal(x["result"])
	}
	return mclean.m.Marshal(v)
}

// NewDecoder shims runtime.Marshaler.NewDecoder
func (mclean *MarshalClean) NewDecoder(r io.Reader) runtime.Decoder {
	return mclean.m.NewDecoder(r)
}

// NewEncoder shims runtime.Marshaler.NewEncoder
func (mclean *MarshalClean) NewEncoder(w io.Writer) runtime.Encoder {
	return mclean.m.NewEncoder(w)
}

// Unmarshal shims runtime.Marshaler.Unmarshal
func (mclean *MarshalClean) Unmarshal(data []byte, v interface{}) error {
	return mclean.m.Unmarshal(data, v)
}

// Proxy is a GRPC Arachne proxy
type Proxy struct {
	cancel   context.CancelFunc
	server   *http.Server
	httpPort string
}

// Run starts the server
func (proxy Proxy) Run() {
	log.Printf("HTTP API listening on port: %s\n", proxy.httpPort)
	proxy.server.ListenAndServe()
}

// Stop turns the proxy server off
func (proxy Proxy) Stop() {
	log.Printf("Stopping Server")
	proxy.server.Close()
	proxy.cancel()
}

// NewHTTPProxy creates an HTTP based arachne endpoint on `httpPort` that
// connects to `rpcPort` and serves data from `contentDir`
func NewHTTPProxy(rpcPort string, httpPort string, contentDir string) Proxy {
	//setup RESTful proxy
	marsh := MarshalClean{m: &runtime.JSONPb{OrigName: true}}
	grpcMux := runtime.NewServeMux(runtime.WithMarshalerOption("*", &marsh))
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	//defer cancel()
	opts := []grpc.DialOption{grpc.WithInsecure()}

	log.Println("HTTP proxy connecting to localhost:" + rpcPort)
	err := aql.RegisterQueryHandlerFromEndpoint(ctx, grpcMux, "localhost:"+rpcPort, opts)
	if err != nil {
		fmt.Println("Register Error", err)
	}
	err = aql.RegisterEditHandlerFromEndpoint(ctx, grpcMux, "localhost:"+rpcPort, opts)
	if err != nil {
		fmt.Println("Register Error", err)
	}

	r := mux.NewRouter()

	runtime.OtherErrorHandler = HandleError

	r.PathPrefix("/falcor.json").Handler(falcor.NewHTTPHandler())
	r.PathPrefix("/graphql").Handler(graphql.NewHTTPHandler("localhost:" + rpcPort))

	r.PathPrefix("/v1/").Handler(grpcMux)
	if contentDir != "" {
		r.PathPrefix("/").Handler(http.StripPrefix("/", http.FileServer(http.Dir(contentDir))))
	}

	return Proxy{
		cancel,
		&http.Server{
			Addr:    ":" + httpPort,
			Handler: r,
		},
		httpPort,
	}
}
