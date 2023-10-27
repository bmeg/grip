/*
RESTFUL Gin Web endpoint
*/

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util"
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
		h.WriteVertex(c.Writer, c.Request, c.Param("graph"))
	})
	r.POST(":graph/add-graph", func(c *gin.Context) {
		h.AddGraph(c.Writer, c.Request, c.Param("graph"))
	})
	r.POST(":graph/bulk-load", func(c *gin.Context) {
		h.BulkLoad(c.Writer, c.Request, c.Param("graph"))
	})
	r.DELETE(":graph/del-graph", func(c *gin.Context) {
		h.DeleteGraph(c.Writer, c.Request, c.Param("graph"))
	})
	r.DELETE(":graph/del-edge/:edge-id", func(c *gin.Context) {
		h.DeleteEdge(c.Writer, c.Request, c.Param("graph"), c.Param("edge-id"))
	})
	r.DELETE(":graph/del-vertex/:vertex-id", func(c *gin.Context) {
		h.DeleteVertex(c.Writer, c.Request, c.Param("graph"), c.Param("vertex-id"))
	})
	r.GET(":graph/list-labels", func(c *gin.Context) {
		h.ListLabels(c.Writer, c.Request, c.Param("graph"))
	})
	r.GET(":graph/get-schema", func(c *gin.Context) {
		h.GetSchema(c.Writer, c.Request, c.Param("graph"))
	})
	r.GET(":graph/get-graph", func(c *gin.Context) {
		h.GetGraph(c.Writer, c.Request, c.Param("graph"))
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
	http.Error(writer, fmt.Sprintln("[500]	graph", graph, "error:", err), http.StatusInternalServerError)
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

// not sure what this does might want to delete. Maybe don't need the schema functions in here and do that manually
func (gh *Handler) GetGraph(writer http.ResponseWriter, request *http.Request, graph string) {
	if graph_data, err := gh.client.GetMapping(graph); err != nil {
		RegError(writer, graph, err)
	} else {
		log.WithFields(log.Fields{"graph": graph}).Info(graph_data)
		http.Error(writer, fmt.Sprintln("[200]	GET:", graph, " ", graph_data), http.StatusOK)
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

func (gh *Handler) GetEdge(writer http.ResponseWriter, request *http.Request, graph string, edge string) {
	if edge, err := gh.client.GetEdge(graph, edge); err != nil {
		RegError(writer, graph, err)
	} else {
		log.WithFields(log.Fields{"graph": graph}).Info(edge)
		http.Error(writer, fmt.Sprintln("[200]	GET:", graph, "EDGE:", edge), http.StatusOK)
	}
}

func (gh *Handler) DeleteEdge(writer http.ResponseWriter, request *http.Request, graph string, edge string) {
	if _, err := gh.client.GetEdge(graph, edge); err == nil {
		if err := gh.client.DeleteEdge(graph, edge); err != nil {
			RegError(writer, graph, err)
		} else {
			log.WithFields(log.Fields{"graph": graph}).Info(edge)
			http.Error(writer, fmt.Sprintln("[200]	DELETE:", graph, "EDGE:", edge), http.StatusOK)
		}
	} else {
		RegError(writer, graph, err)
	}
}

func (gh *Handler) DeleteVertex(writer http.ResponseWriter, request *http.Request, graph string, vertex string) {
	if _, err := gh.client.GetVertex(graph, vertex); err == nil {
		if err := gh.client.DeleteVertex(graph, vertex); err != nil {
			RegError(writer, graph, err)
		} else {
			log.WithFields(log.Fields{"graph": graph}).Info(vertex)
			http.Error(writer, fmt.Sprintln("[200]	DELETE:", graph, "VERTEX:", vertex), http.StatusOK)
		}
	} else {
		RegError(writer, graph, err)
	}
}

func (gh *Handler) WriteVertex(writer http.ResponseWriter, request *http.Request, graph string) {
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

func (gh *Handler) BulkLoad(writer http.ResponseWriter, request *http.Request, graph string) error {
	var workerCount = 1
	var logRate = 10000
	log.WithFields(log.Fields{"graph": graph}).Info("loading data")

	var body []byte
	var err error
	json_map := map[string]any{}

	if body, err = io.ReadAll(request.Body); err != nil {
		RegError(writer, graph, err)
		return err
	}

	if body == nil {
		RegError(writer, graph, err)
		return err
	} else {
		if err := json.Unmarshal([]byte(body), &json_map); err != nil {
			RegError(writer, graph, err)
			return err
		}
	}

	elemChan := make(chan *gripql.GraphElement)
	go func() {
		if err := gh.client.BulkAdd(elemChan); err != nil {
			log.Errorf("bulk add error: %v", err)
		}
	}()

	// vertices and edges are expected to be ndjson format
	_, ok := json_map["vertex"]
	if ok {
		vertexFile := json_map["vertex"].(string)
		log.Infof("Loading vertex file: %s", vertexFile)
		count := 0
		vertChan, err := util.StreamVerticesFromFile(vertexFile, workerCount)
		if err != nil {
			log.Infof("ERROR: ", err)
			return err
		}
		for v := range vertChan {
			count++
			if count%logRate == 0 {
				log.Infof("Loaded %d vertices", count)
			}
			elemChan <- &gripql.GraphElement{Graph: graph, Vertex: v}
		}
		log.Infof("Loaded total of %d vertices", count)
	}

	_, ok = json_map["edge"]
	if ok {
		edgeFile := json_map["edge"].(string)
		log.Infof("Loading edge file: %s", edgeFile)
		count := 0
		edgeChan, err := util.StreamEdgesFromFile(edgeFile, workerCount)
		if err != nil {
			return err
		}
		for e := range edgeChan {
			count++
			if count%logRate == 0 {
				log.Infof("Loaded %d edges", count)
			}
			elemChan <- &gripql.GraphElement{Graph: graph, Edge: e}
		}
		log.Infof("Loaded total of %d edges", count)
	}

	close(elemChan)
	return nil
}
