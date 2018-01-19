package falcor

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"
)

type FalcorHandler struct {
}

func NewHTTPHandler() http.Handler {
	return &FalcorHandler{}
}

func (self *FalcorHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	m, _ := url.ParseQuery(request.URL.RawQuery)
	if x, ok := m["paths"]; ok {
		log.Printf("Request: %s", x)
	}
	out := []string{}
	j, _ := json.Marshal(out)
	writer.Write(j)
}
