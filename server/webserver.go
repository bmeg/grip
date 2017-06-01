package arachne

import (
	"fmt"
	"github.com/bmeg/arachne/ophion"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"runtime/debug"
)

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

func (self *MarshalClean) ContentType() string {
	return self.m.ContentType()
}

func (self *MarshalClean) Marshal(v interface{}) ([]byte, error) {
	if x, ok := v.(map[string]proto.Message); ok {
		return self.m.Marshal(x["result"])
	}
	return self.m.Marshal(v)
}

func (self *MarshalClean) NewDecoder(r io.Reader) runtime.Decoder {
	return self.m.NewDecoder(r)
}

func (self *MarshalClean) NewEncoder(w io.Writer) runtime.Encoder {
	return self.m.NewEncoder(w)
}

func (self *MarshalClean) Unmarshal(data []byte, v interface{}) error {
	return self.m.Unmarshal(data, v)
}

func StartHttpProxy(rpcPort string, httpPort string, contentDir string) {
	//setup RESTful proxy
	marsh := MarshalClean{m: &runtime.JSONPb{OrigName: true}}
	grpcMux := runtime.NewServeMux(runtime.WithMarshalerOption("*", &marsh))
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	opts := []grpc.DialOption{grpc.WithInsecure()}

	log.Println("HTTP proxy connecting to localhost:" + rpcPort)
	err := ophion.RegisterQueryHandlerFromEndpoint(ctx, grpcMux, "localhost:"+rpcPort, opts)
	if err != nil {
		fmt.Println("Register Error", err)

	}
	r := mux.NewRouter()

	runtime.OtherErrorHandler = HandleError
	// Routes consist of a path and a handler function
	r.HandleFunc("/",
		func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filepath.Join(contentDir, "index.html"))
		})
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir(contentDir))))

	r.PathPrefix("/v1/").Handler(grpcMux)
	log.Printf("HTTP API listening on port: %s\n", httpPort)
	http.ListenAndServe(":"+httpPort, r)
}
