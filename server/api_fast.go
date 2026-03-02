package server

import (
	"bufio"
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
func writeLineFast(w io.Writer, out map[string]any) error {
	b, err := sonic.ConfigFastest.Marshal(out)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte("\n"))
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

var (
	jsonVertexPrefix = []byte(`{"vertex":{`)
	jsonEdgePrefix   = []byte(`{"edge":{`)
)

func appendRawPayloadObject(dst []byte, raw string, first *bool) []byte {
	if len(raw) >= 2 && raw[0] == '{' && raw[len(raw)-1] == '}' {
		if len(raw) > 2 {
			if !*first {
				dst = append(dst, ',')
			}
			dst = append(dst, raw[1:len(raw)-1]...)
			*first = false
		}
		return dst
	}
	return appendJSONField(dst, "_raw", raw, first)
}

func writeVertexElementFast(w io.Writer, v gdbi.Row) error {
	if v == nil {
		return nil
	}
	raw := v.GetRaw()
	if raw != "" {
		out := make([]byte, 0, len(raw)+96)
		out = append(out, jsonVertexPrefix...)
		first := true
		if v.GetID() != "" {
			out = appendJSONField(out, "_id", v.GetID(), &first)
		}
		if v.GetLabel() != "" {
			out = appendJSONField(out, "_label", v.GetLabel(), &first)
		}
		out = appendRawPayloadObject(out, raw, &first)
		out = append(out, '}', '}', '\n')
		_, err := w.Write(out)
		return err
	}

	dataBytes, err := sonic.ConfigFastest.Marshal(v.GetPayload())
	if err != nil {
		return err
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
	if v.GetID() != "" {
		out = appendJSONField(out, "_id", v.GetID(), &first)
	}
	if v.GetLabel() != "" {
		out = appendJSONField(out, "_label", v.GetLabel(), &first)
	}
	out = append(out, '}', '}', '\n')
	_, err = w.Write(out)
	return err
}

func writeEdgeElementFast(w io.Writer, e gdbi.Row) error {
	if e == nil {
		return nil
	}
	raw := e.GetRaw()
	if raw != "" {
		out := make([]byte, 0, len(raw)+128)
		out = append(out, jsonEdgePrefix...)
		first := true
		if e.GetID() != "" {
			out = appendJSONField(out, "_id", e.GetID(), &first)
		}
		if e.GetLabel() != "" {
			out = appendJSONField(out, "_label", e.GetLabel(), &first)
		}
		if e.GetFrom() != "" {
			out = appendJSONField(out, "_from", e.GetFrom(), &first)
		}
		if e.GetTo() != "" {
			out = appendJSONField(out, "_to", e.GetTo(), &first)
		}
		out = appendRawPayloadObject(out, raw, &first)
		out = append(out, '}', '}', '\n')
		_, err := w.Write(out)
		return err
	}

	dataBytes, err := sonic.ConfigFastest.Marshal(e.GetPayload())
	if err != nil {
		return err
	}
	out := make([]byte, 0, len(dataBytes)+96)
	out = append(out, `{"edge":{`...)
	first := true
	if len(dataBytes) > 2 && dataBytes[0] == '{' && dataBytes[len(dataBytes)-1] == '}' {
		if len(dataBytes) > 2 {
			out = append(out, dataBytes[1:len(dataBytes)-1]...)
			first = false
		}
	}
	if e.GetID() != "" {
		out = appendJSONField(out, "_id", e.GetID(), &first)
	}
	if e.GetLabel() != "" {
		out = appendJSONField(out, "_label", e.GetLabel(), &first)
	}
	if e.GetFrom() != "" {
		out = appendJSONField(out, "_from", e.GetFrom(), &first)
	}
	if e.GetTo() != "" {
		out = appendJSONField(out, "_to", e.GetTo(), &first)
	}
	out = append(out, '}', '}', '\n')
	_, err = w.Write(out)
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
	bufw := bufio.NewWriterSize(resp, 256*1024)

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
			v := t.GetCurrent()
			if v != nil {
				if v.Mode() == gdbi.RowModeReference || (v.Mode() == gdbi.RowModeUnknown && !v.IsLoaded() && v.GetRaw() == "") {
					v = graph.GetVertex(v.GetID(), true)
				}
				if v != nil {
					err = writeVertexElementFast(bufw, v)
				}
			}
		case gdbi.EdgeData:
			e := t.GetCurrent()
			if e != nil {
				if e.Mode() == gdbi.RowModeReference || (e.Mode() == gdbi.RowModeUnknown && !e.IsLoaded() && e.GetRaw() == "") {
					e = graph.GetEdge(e.GetID(), true)
				}
				if e != nil {
					err = writeEdgeElementFast(bufw, e)
				}
			}
		case gdbi.CountData:
			err = writeLineFast(bufw, map[string]any{"count": t.GetCount()})
		case gdbi.AggregationData:
			agg := t.GetAggregation()
			aggMap := map[string]any{
				"name":  agg.Name,
				"key":   agg.Key,
				"value": agg.Value,
			}
			err = writeLineFast(bufw, map[string]any{"aggregations": aggMap})
		case gdbi.RenderData:
			err = writeLineFast(bufw, map[string]any{"render": t.GetRender()})
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
			err = writeLineFast(bufw, map[string]any{"path": o})
		default:
			// Just use the normal protobuf batch convert if we don't know what it is
		}

		if err != nil {
			log.Errorf("fastQueryHandler: %v", err)
			break
		}
		rowsSent++
		if rowsSent%512 == 0 {
			if err := bufw.Flush(); err != nil {
				log.Errorf("fastQueryHandler flush: %v", err)
				break
			}
		}
	}

	if err := bufw.Flush(); err != nil {
		log.Errorf("fastQueryHandler final flush: %v", err)
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
