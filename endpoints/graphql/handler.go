/*
GraphQL Web endpoint
*/

package main

import (
	"fmt"
	"net/http"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/graphql-go/handler"
)

// handle the graphql queries for a single endpoint
type graphHandler struct {
	graph      string
	gqlHandler *handler.Handler
	timestamp  string
	client     gripql.Client
	//schema     *gripql.Graph
}

// Handler is a GraphQL endpoint to query the Grip database
type Handler struct {
	handlers map[string]*graphHandler
	client   gripql.Client
}

// NewClientHTTPHandler initilizes a new GraphQLHandler
func NewHTTPHandler(client gripql.Client, config map[string]string) (http.Handler, error) {
	h := &Handler{
		client:   client,
		handlers: map[string]*graphHandler{},
	}
	return h, nil
}

// Static HTML that links to Apollo GraphQL query editor
var sandBox = `
<div id="sandbox" style="position:absolute;top:0;right:0;bottom:0;left:0"></div>
<script src="https://embeddable-sandbox.cdn.apollographql.com/_latest/embeddable-sandbox.umd.production.min.js"></script>
<script>
 new window.EmbeddedSandbox({
   target: "#sandbox",
   // Pass through your server href if you are embedding on an endpoint.
   // Otherwise, you can pass whatever endpoint you want Sandbox to start up with here.
   initialEndpoint: window.location.href,
 });
 // advanced options: https://www.apollographql.com/docs/studio/explorer/sandbox#embedding-sandbox
</script>`

// ServeHTTP responds to HTTP graphql requests
func (gh *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	//log.Infof("Request for %s", request.URL.Path)
	//If no graph provided, return the Query Editor page
	if request.URL.Path == "" || request.URL.Path == "/" {
		writer.Write([]byte(sandBox))
		return
	}
	//pathRE := regexp.MustCompile("/(.+)$")
	//graphName := pathRE.FindStringSubmatch(request.URL.Path)[1]
	graphName := request.URL.Path
	var handler *graphHandler
	var ok bool
	if handler, ok = gh.handlers[graphName]; ok {
		//Call the setup function. If nothing has changed it will return without doing anything
		err := handler.setup()
		if err != nil {
			http.Error(writer, fmt.Sprintf("No GraphQL handler found for graph: %s", graphName), http.StatusInternalServerError)
			return
		}
	} else {
		//Graph handler was not found, so we'll need to set it up
		var err error
		handler, err = newGraphHandler(graphName, gh.client)
		if err != nil {
			http.Error(writer, fmt.Sprintf("No GraphQL handler found for graph: %s", graphName), http.StatusInternalServerError)
			return
		}
		gh.handlers[graphName] = handler
	}
	if handler != nil && handler.gqlHandler != nil {
		handler.gqlHandler.ServeHTTP(writer, request)
	} else {
		http.Error(writer, fmt.Sprintf("No GraphQL handler found for graph: %s", graphName), http.StatusInternalServerError)
	}
}

// newGraphHandler creates a new graphql handler from schema
func newGraphHandler(graph string, client gripql.Client) (*graphHandler, error) {
	o := &graphHandler{
		graph:  graph,
		client: client,
	}
	err := o.setup()
	if err != nil {
		return nil, err
	}
	return o, nil
}

// check timestamp to see if schema needs to be updated, and if so
// rebuild graphql schema
func (gh *graphHandler) setup() error {
	ts, _ := gh.client.GetTimestamp(gh.graph)
	if ts == nil || ts.Timestamp != gh.timestamp {
		log.WithFields(log.Fields{"graph": gh.graph}).Info("Reloading GraphQL schema")
		schema, err := gh.client.GetSchema(gh.graph)
		if err != nil {
			log.WithFields(log.Fields{"graph": gh.graph, "error": err}).Error("GetSchema error")
			return err
		}
		gqlSchema, err := buildGraphQLSchema(schema, gh.client, gh.graph)
		if err != nil {
			log.WithFields(log.Fields{"graph": gh.graph, "error": err}).Error("GraphQL schema build failed")
			gh.gqlHandler = nil
			gh.timestamp = ""
		} else {
			log.WithFields(log.Fields{"graph": gh.graph}).Info("Built GraphQL schema")
			gh.gqlHandler = handler.New(&handler.Config{
				Schema: gqlSchema,
			})
			gh.timestamp = ts.Timestamp
		}
	}
	return nil
}
