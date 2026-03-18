/*
GraphQL Web endpoint
*/

//go:generate ./generate.sh

package cypher

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"regexp"

	"github.com/bmeg/grip/cypher/compiler"
	"github.com/bmeg/grip/gripql"
	"google.golang.org/protobuf/encoding/protojson"

	log "github.com/sirupsen/logrus"
)

// Handler is a GraphQL endpoint to query the Grip database
type Handler struct {
	client gripql.Client
}

// NewHTTPHandler initilizes a new GraphQLHandler
func NewHTTPHandler(client gripql.Client) (http.Handler, error) {
	h := &Handler{
		client: client,
	}
	return h, nil
}

// ServeHTTP responds to HTTP graphql requests
func (gh *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()
	pathRE := regexp.MustCompile("/cypher/(.*)$")
	parts := pathRE.FindStringSubmatch(request.URL.Path)
	if len(parts) < 2 {
		http.Error(writer, "invalid cypher path", http.StatusBadRequest)
		return
	}
	graphName := parts[1]

	if request.Method != "POST" {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(writer, fmt.Sprintf("failed to read request body: %s", err), http.StatusBadRequest)
		return
	}

	buf := bytes.Buffer{}
	buf.Write(body)
	cyQuery := buf.String()
	gripQuery, err := compiler.RunParser(cyQuery)
	if err != nil {
		log.Printf("Parse Error: %s", err)
		http.Error(writer, fmt.Sprintf("failed to parse query: %s", err), http.StatusInternalServerError)
		return
	}
	log.Printf("Cypher Query: %s, %s = %s", graphName, cyQuery, gripQuery.String())

	result, err := gh.client.Traversal(ctx,
		&gripql.GraphQuery{
			Graph: graphName,
			Query: gripQuery.Statements,
		},
	)
	if err != nil {
		log.Printf("Query Error: %s", err)
		http.Error(writer, fmt.Sprintf("failed to execute query: %s", err), http.StatusInternalServerError)
		return
	}

	for row := range result {
		rowBytes, err := protojson.Marshal(row)
		if err != nil {
			log.Printf("Marshal Error: %s", err)
			continue
		}
		writer.Write(rowBytes)
		writer.Write([]byte("\n"))
	}
}
