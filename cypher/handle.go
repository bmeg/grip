/*
GraphQL Web endpoint
*/

package cypher

import (
	"bytes"
	"net/http"
	"regexp"

	"github.com/bmeg/grip/gripql"

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
	pathRE := regexp.MustCompile("/cypher/(.*)$")
	graphName := pathRE.FindStringSubmatch(request.URL.Path)[1]
	if request.Method == "POST" {
		buf := bytes.Buffer{}
		buf.ReadFrom(request.Body)
		cyQuery := buf.String()
		gripQuery := RunParser(cyQuery)
		log.Printf("Cypher Query: %s, %s = %s", graphName, cyQuery, gripQuery.String())
	}
}
