/*
GQL Web endpoint
*/

//go:generate ./generate.sh

package gql

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/bmeg/grip/endpoints/gql/compiler"
	"github.com/bmeg/grip/gripql"
	"google.golang.org/protobuf/encoding/protojson"

	log "github.com/sirupsen/logrus"
)

// Handler is a GQL endpoint to query the Grip database.
type Handler struct {
	client gripql.Client
}

// NewHTTPHandler initializes a new GQL handler.
func NewHTTPHandler(client gripql.Client) (http.Handler, error) {
	h := &Handler{
		client: client,
	}
	return h, nil
}

// ServeHTTP responds to HTTP GQL requests.
func (gh *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()
	pathRE := regexp.MustCompile("/gql/(.*)$")
	parts := pathRE.FindStringSubmatch(request.URL.Path)
	if len(parts) < 2 {
		http.Error(writer, "invalid gql path", http.StatusBadRequest)
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

	var requestPayload struct {
		Query string `json:"query"`
	}
	if err := json.Unmarshal(body, &requestPayload); err != nil {
		http.Error(writer, fmt.Sprintf("failed to parse request body as JSON: %s", err), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(requestPayload.Query) == "" {
		http.Error(writer, "missing query in request body", http.StatusBadRequest)
		return
	}

	gqlQuery := requestPayload.Query
	gripQuery, err := compiler.RunParser(gqlQuery)
	if err != nil {
		log.Printf("Parse Error: %s", err)
		http.Error(writer, fmt.Sprintf("failed to parse query: %s", err), http.StatusInternalServerError)
		return
	}
	log.Printf("GQL Query: %s, %s = %s", graphName, gqlQuery, gripQuery.String())

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
		rowBytes, err := protojson.Marshal(row.GetRender())
		if err != nil {
			log.Printf("Marshal Error: %s", err)
			continue
		}
		writer.Write(rowBytes)
		writer.Write([]byte("\n"))
	}
}
