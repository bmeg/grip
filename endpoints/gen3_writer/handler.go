/*
RESTFUL Gin Web endpoint
*/

package main

import (
	"fmt"
	"io"
	"net/http"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/encoding/protojson"
)

type Handler struct {
	router *gin.Engine
	client gripql.Client
}

func NewHTTPHandler(client gripql.Client) (http.Handler, error) {
	r := gin.Default()
	h := &Handler{
		router: r,
		client: client,
	}

	r.POST(":graph/add-vertex", func(c *gin.Context) {
		h.WriteVertex(c.Writer, c.Request, c.Param("graph"), c)
	})
	r.POST(":graph/add-graph", func(c *gin.Context) {
		h.AddGraph(c.Writer, c.Request, c.Param("graph"))
	})
	r.DELETE(":graph/del-graph", func(c *gin.Context) {
		h.DeleteGraph(c.Writer, c.Request, c.Param("graph"))
	})
	r.GET(":graph/list-labels", func(c *gin.Context) {
		h.ListLabels(c.Writer, c.Request, c.Param("graph"))
	})
	r.GET(":graph/get-schema", func(c *gin.Context) {
		h.GetSchema(c.Writer, c.Request, c.Param("graph"))
	})
	r.GET(":graph/get-vertex/:vertex-id", func(c *gin.Context) {
		h.GetVertex(c.Writer, c.Request, c.Param("graph"), c.Param("vertex-id"))
	})
	r.GET(":graph", func(c *gin.Context) {
		if c.Param("graph") == "list-graphs" {
			h.ListGraphs(c.Writer, c.Request)
		}
	})
	return h, nil
}

// ServeHTTP responds to HTTP graphql requests
func (gh *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	/*fmt.Println("REQUEST", request)
	fmt.Println("WRITER", writer)*/
	gh.router.ServeHTTP(writer, request)
}

func RegError(writer http.ResponseWriter, graph string, err error) {
	log.WithFields(log.Fields{"graph": graph, "error": err})
	http.Error(writer, fmt.Sprintln("graph", graph, "error:", err), http.StatusInternalServerError)
}

func (gh *Handler) ListLabels(writer http.ResponseWriter, request *http.Request, graph string) {
	if labels, err := gh.client.ListLabels(graph); err != nil {
		RegError(writer, graph, err)
	} else {
		log.WithFields(log.Fields{"graph": graph}).Info(labels)
		http.Error(writer, fmt.Sprintln("[200]	GET:", graph, " ", labels), http.StatusOK)
	}
}

func (gh *Handler) GetSchema(writer http.ResponseWriter, request *http.Request, graph string) {
	if schema, err := gh.client.GetSchema(graph); err != nil {
		RegError(writer, graph, err)
	} else {
		log.WithFields(log.Fields{"graph": graph}).Info(schema)
		http.Error(writer, fmt.Sprintln("[200]	GET:", graph, " ", schema), http.StatusOK)
	}
}

func (gh *Handler) ListGraphs(writer http.ResponseWriter, request *http.Request) {
	if graphs, err := gh.client.ListGraphs(); err != nil {
		RegError(writer, "", err)
	} else {
		log.WithFields(log.Fields{}).Info(graphs)
		http.Error(writer, fmt.Sprintln("[200]	GET:", graphs), http.StatusOK)
	}
}

func (gh *Handler) AddGraph(writer http.ResponseWriter, request *http.Request, graph string) {
	if err := gh.client.AddGraph(graph); err != nil {
		RegError(writer, graph, err)
	} else {
		log.WithFields(log.Fields{}).Info("[200]	POST:", graph)
		http.Error(writer, fmt.Sprintln("[200]	POST:", graph), http.StatusOK)
	}
}

func (gh *Handler) DeleteGraph(writer http.ResponseWriter, request *http.Request, graph string) {
	if err := gh.client.DeleteGraph(graph); err != nil {
		RegError(writer, graph, err)
	} else {
		log.WithFields(log.Fields{}).Info("[200]	DELETE:", graph)
		http.Error(writer, fmt.Sprintln("[200]	DELETE:", graph), http.StatusOK)
	}
}

func (gh *Handler) GetVertex(writer http.ResponseWriter, request *http.Request, graph string, vertex string) {
	if vertex, err := gh.client.GetVertex(graph, vertex); err != nil {
		RegError(writer, graph, err)
	} else {
		log.WithFields(log.Fields{"graph": graph}).Info(vertex)
		http.Error(writer, fmt.Sprintln("[200]	GET:", graph, "VERTEX:", vertex), http.StatusOK)
	}
}

func (gh *Handler) WriteVertex(writer http.ResponseWriter, request *http.Request, graph string, c *gin.Context) {
	var body []byte
	var err error
	v := &gripql.Vertex{}

	if body, err = io.ReadAll(request.Body); err != nil {
		RegError(writer, graph, err)
		return
	}
	if body == nil {
		RegError(writer, graph, err)
		return
	} else {
		if err := protojson.Unmarshal([]byte(body), v); err != nil {
			RegError(writer, graph, err)
			return
		}
	}
	if err := gh.client.AddVertex(graph, v); err != nil {
		RegError(writer, graph, err)
	} else {
		log.WithFields(log.Fields{"graph": graph}).Info("[200]	POST	VERTEX: ", v)
		http.Error(writer, fmt.Sprintln("[200]	POST	VERTEX: ", v), http.StatusOK)
	}
}
