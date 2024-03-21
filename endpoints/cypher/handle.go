/*
GraphQL Web endpoint
*/

package main

import (
	"bytes"
	"context"
	"io"
	"net/http"

	"github.com/bmeg/grip/endpoints/cypher/translate"
	"github.com/bmeg/grip/gripql"
	"google.golang.org/protobuf/encoding/protojson"

	log "github.com/sirupsen/logrus"
)

// Handler is a GraphQL endpoint to query the Grip database
type Handler struct {
	client gripql.Client
}

// NewHTTPHandler initilizes a new GraphQLHandler
func NewHTTPHandler(client gripql.Client, config map[string]string) (http.Handler, error) {
	h := &Handler{
		client: client,
	}
	return h, nil
}

// ServeHTTP responds to HTTP graphql requests
func (gh *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	graphName := request.URL.Path
	if request.Method == "POST" {
		buf := bytes.Buffer{}
		buf.ReadFrom(request.Body)
		cyQuery := buf.String()
		gripQuery, err := translate.RunParser(cyQuery)
		if err != nil {
			log.Printf("Parse Error: %s", err)
		}
		log.Printf("Cypher Query: %s, %s = %s", graphName, cyQuery, gripQuery.String())
		client, err := gh.client.QueryC.Traversal(context.Background(), &gripql.GraphQuery{Graph: graphName, Query: gripQuery.Statements})
		if err != nil {
			log.Printf("Parse Error: %s", err)
		}
		for {
			t, err := client.Recv()
			if err == io.EOF {
				return
			}
			if b, err := protojson.Marshal(t); err == nil {
				writer.Write(b)
				writer.Write([]byte("\n"))
			}
		}
	}
}
