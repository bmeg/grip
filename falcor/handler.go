/*
Prototype Falcor support. Not yet working
*/

package falcor

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"
)

// Handler is a test HTTP handler to implements a Falcor endpoint on a Arachne backend
type Handler struct {
}

// NewHTTPHandler creates a new FalcorHandler
func NewHTTPHandler() http.Handler {
	return &Handler{}
}

// ServeHTTP respond to HTTP Falcor requests
func (falcor *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	m, _ := url.ParseQuery(request.URL.RawQuery)
	if x, ok := m["paths"]; ok {
		log.Printf("Request: %s", x)
	}
	out := []string{}
	j, _ := json.Marshal(out)
	writer.Write(j)
}
