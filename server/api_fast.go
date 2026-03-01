package server

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bmeg/grip/accounts"
	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/engine/pipeline"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"google.golang.org/protobuf/encoding/protojson"
)

// writeLineFast writes a line to the HTTP ResponseWriter using sonic
func writeLineFast(resp http.ResponseWriter, out map[string]any) error {
	b, err := sonic.ConfigFastest.Marshal(out)
	if err != nil {
		return err
	}
	_, err = resp.Write(b)
	if err != nil {
		return err
	}
	_, err = resp.Write([]byte("\n"))
	return err
}

func appendJSONField(dst []byte, key string, val string, first *bool) []byte {
	if !*first {
		dst = append(dst, ',')
	}
	*first = false
	dst = append(dst, '"')
	dst = append(dst, key...)
	dst = append(dst, '"', ':')
	dst = strconv.AppendQuote(dst, val)
	return dst
}

func writeVertexElementFast(resp http.ResponseWriter, v *gdbi.DataElement) error {
	if v == nil {
		return nil
	}
	var dataBytes []byte
	if v.RawJSON != "" {
		dataBytes = []byte(v.RawJSON)
	} else {
		var err error
		dataBytes, err = sonic.ConfigFastest.Marshal(v.Data)
		if err != nil {
			return err
		}
	}

	out := make([]byte, 0, len(dataBytes)+64)
	out = append(out, `{"vertex":{`...)
	first := true
	if len(dataBytes) > 2 && dataBytes[0] == '{' && dataBytes[len(dataBytes)-1] == '}' {
		if len(dataBytes) > 2 {
			out = append(out, dataBytes[1:len(dataBytes)-1]...)
			first = false
		}
	}
	if v.ID != "" {
		out = appendJSONField(out, "_id", v.ID, &first)
	}
	if v.Label != "" {
		out = appendJSONField(out, "_label", v.Label, &first)
	}
	out = append(out, '}', '}', '\n')
	_, err := resp.Write(out)
	return err
}

func (server *GripServer) fastQueryHandler(resp http.ResponseWriter, req *http.Request, graphName string) {
	// Authentication
	md := accounts.MetaData{}
	for k, v := range req.Header {
		md[strings.ToLower(k)] = v
	}

	// Check auth
	auth := server.conf.Server.Accounts.GetAuth()
	access := server.conf.Server.Accounts.GetAccess()

	user, err := auth.Validate(md)
	if err != nil {
		http.Error(resp, "PermissionDenied", http.StatusUnauthorized)
		return
	}
	err = access.Enforce(user, graphName, accounts.Query)
	if err != nil {
		http.Error(resp, "PermissionDenied", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}

	query := gripql.GraphQuery{}
	err = protojson.Unmarshal(body, &query)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}

	gdb, err := server.getGraphDB(graphName)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}
	graph, err := gdb.Graph(graphName)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}

	compiler := graph.Compiler()
	compiledPipeline, err := compiler.Compile(query.Query, nil)
	if err != nil {
		http.Error(resp, err.Error(), http.StatusBadRequest)
		return
	}

	resp.Header().Set("Content-Type", "application/x-ndjson")
	resp.WriteHeader(http.StatusOK)

	ctx := req.Context()
	man := engine.NewManager(server.conf.Server.WorkDir)
	defer man.Cleanup()

	// 20k buffer size used internally by engine
	pipe := pipeline.Start(ctx, compiledPipeline, man, 20000, nil, nil)
	if pipe == nil {
		return
	}

	dataType := compiledPipeline.DataType()

	start := time.Now()
	var rowsSent int

	for t := range pipe.Outputs {
		if t.IsSignal() {
			continue
		}
		var err error
		switch dataType {
		case gdbi.VertexData:
			cur := t.GetCurrent()
			if cur != nil {
				v := cur.Get()
				if v != nil {
					if !v.Loaded {
						v = graph.GetVertex(v.ID, true)
					}
					if v != nil {
						err = writeVertexElementFast(resp, v)
					}
				}
			}
		case gdbi.EdgeData:
			cur := t.GetCurrent()
			if cur != nil {
				e := cur.Get()
				if e != nil {
					if !e.Loaded {
						e = graph.GetEdge(e.ID, true)
					}
					if e != nil {
						err = writeLineFast(resp, map[string]any{"edge": e.ToDict()})
					}
				}
			}
		case gdbi.CountData:
			err = writeLineFast(resp, map[string]any{"count": t.GetCount()})
		case gdbi.AggregationData:
			agg := t.GetAggregation()
			aggMap := map[string]any{
				"name":  agg.Name,
				"key":   agg.Key,
				"value": agg.Value,
			}
			err = writeLineFast(resp, map[string]any{"aggregations": aggMap})
		case gdbi.RenderData:
			err = writeLineFast(resp, map[string]any{"render": t.GetRender()})
		case gdbi.PathData:
			path := t.GetPath()
			o := make([]any, len(path))
			for i := range path {
				j := map[string]any{}
				if path[i].Vertex != "" {
					j["vertex"] = path[i].Vertex
				} else if path[i].Edge != "" {
					j["edge"] = path[i].Edge
				}
				o[i] = j
			}
			err = writeLineFast(resp, map[string]any{"path": o})
		default:
			// Just use the normal protobuf batch convert if we don't know what it is
		}

		if err != nil {
			log.Errorf("fastQueryHandler: %v", err)
			break
		}
		rowsSent++
	}

	elapsed := time.Since(start)
	if rowsSent > 0 {
		rps := float64(rowsSent) / elapsed.Seconds()
		log.Debugf("TraversalFast summary graph=%s rows=%d rps=%.0f total=%s",
			graphName, rowsSent, rps, elapsed.Round(time.Millisecond))
	} else {
		log.Debugf("TraversalFast summary graph=%s rows=0 total=%s",
			graphName, elapsed.Round(time.Millisecond))
	}
}
