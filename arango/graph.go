package arango

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/timestamp"
	"github.com/bmeg/grip/util"
)

const (
	fieldID    = "_key"
	fieldLabel = "_label"
	fieldFrom  = "from"
	fieldTo    = "to"
)

type Graph struct {
	ar        *GraphDB
	ts        *timestamp.Timestamp
	graph     string
	batchSize int
}

func (g *Graph) Compiler() gdbi.Compiler {
	return core.NewCompiler(g, core.IndexStartOptimize)
}

func (g *Graph) GetTimestamp() string {
	if g.ts == nil {
		return ""
	}
	return g.ts.Get(g.graph)
}

func trimSystemFields(data map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range data {
		if strings.HasPrefix(k, "_") || k == fieldFrom || k == fieldTo {
			continue
		}
		out[k] = v
	}
	return out
}

func unpackVertex(doc map[string]any) *gdbi.Vertex {
	v := &gdbi.Vertex{Data: map[string]any{}, Loaded: true}
	if id, ok := doc[fieldID].(string); ok {
		v.ID = id
	}
	if label, ok := doc[fieldLabel].(string); ok {
		v.Label = label
	}
	for k, val := range trimSystemFields(doc) {
		v.Data[k] = val
	}
	return v
}

func unpackEdge(doc map[string]any) *gdbi.Edge {
	e := &gdbi.Edge{Data: map[string]any{}, Loaded: true}
	if id, ok := doc[fieldID].(string); ok {
		e.ID = id
	}
	if label, ok := doc[fieldLabel].(string); ok {
		e.Label = label
	}
	if from, ok := doc[fieldFrom].(string); ok {
		e.From = from
	}
	if to, ok := doc[fieldTo].(string); ok {
		e.To = to
	}
	for k, val := range trimSystemFields(doc) {
		e.Data[k] = val
	}
	return e
}

func (g *Graph) GetVertex(key string, load bool) *gdbi.Vertex {
	res, err := g.ar.queryMaps(
		"FOR v IN @@v FILTER v._key == @id LIMIT 1 RETURN v",
		map[string]any{"@v": vertexCollection(g.graph), "id": key},
	)
	if err != nil || len(res) == 0 {
		return nil
	}
	v := unpackVertex(res[0])
	if !load {
		v.Data = map[string]any{}
	}
	return v
}

func (g *Graph) GetEdge(key string, load bool) *gdbi.Edge {
	res, err := g.ar.queryMaps(
		"FOR e IN @@e FILTER e._key == @id LIMIT 1 RETURN e",
		map[string]any{"@e": edgeCollection(g.graph), "id": key},
	)
	if err != nil || len(res) == 0 {
		return nil
	}
	e := unpackEdge(res[0])
	if !load {
		e.Data = map[string]any{}
	}
	return e
}

func (g *Graph) AddVertex(vertices []*gdbi.Vertex) error {
	for _, v := range vertices {
		if v == nil {
			continue
		}
		if err := g.ar.execAQL(
			`UPSERT { _key: @key }
			 INSERT MERGE({ _key: @key, _label: @label }, @data)
			 UPDATE MERGE({ _label: @label }, @data)
			 IN @@v`,
			map[string]any{
				"@v":    vertexCollection(g.graph),
				"key":   v.ID,
				"label": v.Label,
				"data":  v.Data,
			},
		); err != nil {
			return err
		}
	}
	if g.ts != nil {
		g.ts.Touch(g.graph)
	}
	return nil
}

func (g *Graph) AddEdge(edges []*gdbi.Edge) error {
	for _, e := range edges {
		if e == nil {
			continue
		}
		if err := g.ar.execAQL(
			`UPSERT { _key: @key }
			 INSERT MERGE({ _key: @key, _label: @label, from: @from, to: @to }, @data)
			 UPDATE MERGE({ _label: @label, from: @from, to: @to }, @data)
			 IN @@e`,
			map[string]any{
				"@e":    edgeCollection(g.graph),
				"key":   e.ID,
				"label": e.Label,
				"from":  e.From,
				"to":    e.To,
				"data":  e.Data,
			},
		); err != nil {
			return err
		}
	}
	if g.ts != nil {
		g.ts.Touch(g.graph)
	}
	return nil
}

func (g *Graph) StreamEdges(edgeChan <-chan *gdbi.Edge, batchSize int) error {
	batch := make([]*gdbi.Edge, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := g.AddEdge(batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for edge := range edgeChan {
		batch = append(batch, edge)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func (g *Graph) StreamVertices(vertChan <-chan *gdbi.Vertex, batchSize int) error {
	batch := make([]*gdbi.Vertex, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := g.AddVertex(batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for v := range vertChan {
		batch = append(batch, v)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func (g *Graph) BulkAdd(stream <-chan *gdbi.GraphElement) error {
	return util.StreamBatch(stream, g.batchSize, g.graph, g.StreamVertices, g.StreamEdges)
}

func (g *Graph) BulkDel(data *gdbi.DeleteData) error {
	if data == nil {
		return nil
	}
	if len(data.Edges) > 0 {
		if err := g.ar.execAQL(
			"FOR k IN @keys REMOVE { _key: k } IN @@e OPTIONS { ignoreErrors: true }",
			map[string]any{"@e": edgeCollection(g.graph), "keys": data.Edges},
		); err != nil {
			return err
		}
	}
	if len(data.Vertices) > 0 {
		if err := g.ar.execAQL(
			"FOR e IN @@e FILTER e.from IN @ids OR e.to IN @ids REMOVE e IN @@e",
			map[string]any{"@e": edgeCollection(g.graph), "ids": data.Vertices},
		); err != nil {
			return err
		}
		if err := g.ar.execAQL(
			"FOR k IN @keys REMOVE { _key: k } IN @@v OPTIONS { ignoreErrors: true }",
			map[string]any{"@v": vertexCollection(g.graph), "keys": data.Vertices},
		); err != nil {
			return err
		}
	}
	if g.ts != nil {
		g.ts.Touch(g.graph)
	}
	return nil
}

func (g *Graph) DelVertex(key string) error {
	if err := g.BulkDel(&gdbi.DeleteData{Graph: g.graph, Vertices: []string{key}}); err != nil {
		return fmt.Errorf("failed to delete vertex %s: %w", key, err)
	}
	return nil
}

func (g *Graph) DelEdge(key string) error {
	if err := g.BulkDel(&gdbi.DeleteData{Graph: g.graph, Edges: []string{key}}); err != nil {
		return fmt.Errorf("failed to delete edge %s: %w", key, err)
	}
	return nil
}

func (g *Graph) AddVertexIndex(label string, field string) error {
	return nil
}

func (g *Graph) DeleteVertexIndex(label string, field string) error {
	return nil
}

func (g *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	out := make(chan *gripql.IndexID)
	close(out)
	return out
}

func (g *Graph) VertexLabelScan(ctx context.Context, label string) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		res, err := g.ar.queryMaps(
			"FOR v IN @@v FILTER v._label == @label RETURN { id: v._key }",
			map[string]any{"@v": vertexCollection(g.graph), "label": label},
		)
		if err != nil {
			return
		}
		for _, row := range res {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if id, ok := row["id"].(string); ok {
				out <- id
			}
		}
	}()
	return out
}

func (g *Graph) ListVertexLabels() ([]string, error) {
	res, err := g.ar.queryMaps(
		"FOR v IN @@v COLLECT l = v._label RETURN { label: l }",
		map[string]any{"@v": vertexCollection(g.graph)},
	)
	if err != nil {
		return nil, err
	}
	labels := make([]string, 0, len(res))
	for _, row := range res {
		if label, ok := row["label"].(string); ok && label != "" {
			labels = append(labels, label)
		}
	}
	return labels, nil
}

func (g *Graph) ListEdgeLabels() ([]string, error) {
	res, err := g.ar.queryMaps(
		"FOR e IN @@e COLLECT l = e._label RETURN { label: l }",
		map[string]any{"@e": edgeCollection(g.graph)},
	)
	if err != nil {
		return nil, err
	}
	labels := make([]string, 0, len(res))
	for _, row := range res {
		if label, ok := row["label"].(string); ok && label != "" {
			labels = append(labels, label)
		}
	}
	return labels, nil
}

func (g *Graph) GetVertexList(ctx context.Context, load bool) <-chan *gdbi.Vertex {
	out := make(chan *gdbi.Vertex, 100)
	go func() {
		defer close(out)
		res, err := g.ar.queryMaps(
			"FOR v IN @@v RETURN v",
			map[string]any{"@v": vertexCollection(g.graph)},
		)
		if err != nil {
			return
		}
		for _, doc := range res {
			select {
			case <-ctx.Done():
				return
			default:
			}
			v := unpackVertex(doc)
			if !load {
				v.Data = map[string]any{}
			}
			out <- v
		}
	}()
	return out
}

func makeStringSet(list []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, item := range list {
		out[item] = struct{}{}
	}
	return out
}

func (g *Graph) GetVertexChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	batches := gdbi.LookupBatcher(req, g.batchSize, time.Microsecond)
	out := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(out)
		for batch := range batches {
			ids := make([]string, 0, len(batch))
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
				} else {
					ids = append(ids, batch[i].ID)
				}
			}
			rows, err := g.ar.queryMaps(
				"FOR v IN @@v FILTER v._key IN @ids RETURN v",
				map[string]any{"@v": vertexCollection(g.graph), "ids": ids},
			)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("GetVertexChannel")
				continue
			}
			found := map[string]*gdbi.Vertex{}
			for _, row := range rows {
				v := unpackVertex(row)
				if !load {
					v.Data = map[string]any{}
				}
				found[v.ID] = v
			}
			for _, item := range batch {
				if v, ok := found[item.ID]; ok {
					item.Vertex = v
					out <- item
				}
			}
			for _, s := range signals {
				out <- s
			}
		}
	}()
	return out
}

func (g *Graph) getNeighborEdges(reqChan chan gdbi.ElementLookup, dir string, emitNull bool, edgeLabels []string, outEdges bool, load bool) chan gdbi.ElementLookup {
	batches := gdbi.LookupBatcher(reqChan, g.batchSize, time.Microsecond)
	out := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(out)
		for batch := range batches {
			ids := make([]string, 0, len(batch))
			reqMap := map[string][]gdbi.ElementLookup{}
			returnCount := map[string]int{}
			signals := []gdbi.ElementLookup{}
			for i := range batch {
				if batch[i].IsSignal() {
					signals = append(signals, batch[i])
					continue
				}
				ids = append(ids, batch[i].ID)
				reqMap[batch[i].ID] = append(reqMap[batch[i].ID], batch[i])
				returnCount[batch[i].ID] = 0
			}
			if len(ids) == 0 {
				for _, s := range signals {
					out <- s
				}
				continue
			}

			filterField := fieldFrom
			targetField := fieldTo
			if dir == "in" {
				filterField = fieldTo
				targetField = fieldFrom
			}

			bind := map[string]any{"@e": edgeCollection(g.graph), "ids": ids}
			query := "FOR e IN @@e FILTER e." + filterField + " IN @ids"
			if len(edgeLabels) > 0 {
				bind["labels"] = edgeLabels
				query += " FILTER e._label IN @labels"
			}

			if outEdges {
				query += " RETURN { id: e." + filterField + ", edge: e }"
			} else {
				bind["@v"] = vertexCollection(g.graph)
				query += " FOR v IN @@v FILTER v._key == e." + targetField + " RETURN { id: e." + filterField + ", vertex: v }"
			}

			rows, err := g.ar.queryMaps(query, bind)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("getNeighborEdges")
				continue
			}

			for _, row := range rows {
				id, _ := row["id"].(string)
				for _, req := range reqMap[id] {
					if outEdges {
						if edoc, ok := row["edge"].(map[string]any); ok {
							e := unpackEdge(edoc)
							if !load {
								e.Data = map[string]any{}
							}
							req.Edge = e
							returnCount[id]++
							out <- req
						}
					} else {
						if vdoc, ok := row["vertex"].(map[string]any); ok {
							v := unpackVertex(vdoc)
							if !load {
								v.Data = map[string]any{}
							}
							req.Vertex = v
							returnCount[id]++
							out <- req
						}
					}
				}
			}

			if emitNull {
				idSet := makeStringSet(ids)
				for id := range idSet {
					if returnCount[id] == 0 {
						for _, req := range reqMap[id] {
							if outEdges {
								req.Edge = nil
							} else {
								req.Vertex = nil
							}
							out <- req
						}
					}
				}
			}

			for _, s := range signals {
				out <- s
			}
		}
	}()
	return out
}

func (g *Graph) GetOutChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	_ = ctx
	return g.getNeighborEdges(req, "out", emitNull, edgeLabels, false, load)
}

func (g *Graph) GetInChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	_ = ctx
	return g.getNeighborEdges(req, "in", emitNull, edgeLabels, false, load)
}

func (g *Graph) GetOutEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	_ = ctx
	return g.getNeighborEdges(req, "out", emitNull, edgeLabels, true, load)
}

func (g *Graph) GetInEdgeChannel(ctx context.Context, req chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	_ = ctx
	return g.getNeighborEdges(req, "in", emitNull, edgeLabels, true, load)
}
