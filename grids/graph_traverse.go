package grids

import (
	"bytes"
	"context"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/grids/key"
)

func edgeLabelAllowed(labels map[string]struct{}, label string) bool {
	if len(labels) == 0 {
		return true
	}
	_, ok := labels[label]
	return ok
}

func lookupUIDFromRequest(req gdbi.ElementLookup, ggraph *Graph) (uint64, bool) {
	if meta, ok := req.GetLookupMeta(); ok && meta.UID != 0 {
		return meta.UID, true
	}
	if req.ID == "" || ggraph == nil || ggraph.driver == nil {
		return 0, false
	}
	uid, err := ggraph.driver.GetID(req.ID)
	if err != nil {
		return 0, false
	}
	return uid, true
}

// GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (ggraph *Graph) GetOutChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	edgeLabelSet := make(map[string]struct{}, len(edgeLabels))
	for _, label := range edgeLabels {
		edgeLabelSet[label] = struct{}{}
	}
	go func() {
		defer close(o)
		ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			var batch []gdbi.ElementLookup
			for req := range reqChan {
				if req.IsSignal() {
					if len(batch) > 0 {
						ggraph.resolveBatch(ctx, batch, o, false)
						batch = nil
					}
					o <- req
					continue
				}
				found := false
				uid, ok := lookupUIDFromRequest(req, ggraph)
				if !ok {
					if emitNull {
						req.Vertex = nil
						o <- req
					}
					continue
				}
				skeyPrefix := key.SrcEdgePrefix(uid)
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					_, _, duid, label := key.SrcEdgeKeyParse(it.Key())
					if edgeLabelAllowed(edgeLabelSet, label) {
						if !load {
							dst, _ := ggraph.driver.TranslateID(duid)
							req.Vertex = &gdbi.Vertex{ID: dst, Label: labelFromElementID(dst)}
							o <- req
						} else {
							batch = append(batch, gdbi.ElementLookup{Ref: req.Ref, Meta: gdbi.LookupMeta{UID: duid}})
							if len(batch) >= 1000 {
								ggraph.resolveBatch(ctx, batch, o, false)
								batch = nil
							}
						}
						found = true
					}
				}
				if !found && emitNull {
					req.Vertex = nil
					o <- req
				}
			}
			if len(batch) > 0 {
				ggraph.resolveBatch(ctx, batch, o, false)
			}
			return nil
		})
	}()
	return o
}

// GetInChannel process requests of vertex ids and find the connected vertices on incoming edges
func (ggraph *Graph) GetInChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	edgeLabelSet := make(map[string]struct{}, len(edgeLabels))
	for _, label := range edgeLabels {
		edgeLabelSet[label] = struct{}{}
	}
	go func() {
		defer close(o)
		ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			var batch []gdbi.ElementLookup
			for req := range reqChan {
				if req.IsSignal() {
					if len(batch) > 0 {
						ggraph.resolveBatch(ctx, batch, o, false)
						batch = nil
					}
					o <- req
					continue
				}
				found := false
				uid, ok := lookupUIDFromRequest(req, ggraph)
				if !ok {
					if emitNull {
						req.Vertex = nil
						o <- req
					}
					continue
				}
				dkeyPrefix := key.DstEdgePrefix(uid)
				for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
					_, suid, _, label := key.DstEdgeKeyParse(it.Key())
					if edgeLabelAllowed(edgeLabelSet, label) {
						if !load {
							src, _ := ggraph.driver.TranslateID(suid)
							req.Vertex = &gdbi.Vertex{ID: src, Label: labelFromElementID(src)}
							o <- req
						} else {
							batch = append(batch, gdbi.ElementLookup{Ref: req.Ref, Meta: gdbi.LookupMeta{UID: suid}})
							if len(batch) >= 1000 {
								ggraph.resolveBatch(ctx, batch, o, false)
								batch = nil
							}
						}
						found = true
					}
				}

				if !found && emitNull {
					req.Vertex = nil
					o <- req
				}
			}
			if len(batch) > 0 {
				ggraph.resolveBatch(ctx, batch, o, false)
			}
			return nil
		})
	}()
	return o
}

// GetOutEdgeChannel process requests of vertex ids and find the connected outgoing edges
func (ggraph *Graph) GetOutEdgeChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	edgeLabelSet := make(map[string]struct{}, len(edgeLabels))
	for _, label := range edgeLabels {
		edgeLabelSet[label] = struct{}{}
	}
	go func() {
		defer close(o)
		ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			var batch []gdbi.ElementLookup
			for req := range reqChan {
				if req.IsSignal() {
					if len(batch) > 0 {
						ggraph.resolveBatch(ctx, batch, o, true)
						batch = nil
					}
					o <- req
					continue
				}
				found := false
				uid, ok := lookupUIDFromRequest(req, ggraph)
				if !ok {
					if emitNull {
						req.Edge = nil
						o <- req
					}
					continue
				}
				skeyPrefix := key.SrcEdgePrefix(uid)
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					euid, suid, duid, label := key.SrcEdgeKeyParse(it.Key())
					if edgeLabelAllowed(edgeLabelSet, label) {
						if !load {
							eid, _ := ggraph.driver.TranslateID(euid)
							src, _ := ggraph.driver.TranslateID(suid)
							dst, _ := ggraph.driver.TranslateID(duid)
							e := gdbi.Edge{
								From:  src,
								To:    dst,
								Label: label,
								ID:    eid,
							}
							e.Data = map[string]any{}
							req.Edge = &e
							o <- req
						} else {
							e := gdbi.Edge{Label: label}
							byteVal, _ := it.Value()
							_, loc, data := benchtop.DecodeEdgeValue(byteVal)
							if data != nil {
								e.Data = data
								e.Loaded = true
							}
							batch = append(batch, gdbi.ElementLookup{
								Ref:  req.Ref,
								Edge: &e,
								Meta: gdbi.LookupMeta{Opaque: loc, Data: e.Data, EUID: euid, SUID: suid, DUID: duid},
							})
							if len(batch) >= 1000 {
								ggraph.resolveBatch(ctx, batch, o, true)
								batch = nil
							}
						}
						found = true
					}
				}

				if !found && emitNull {
					req.Edge = nil
					o <- req
				}
			}
			if len(batch) > 0 {
				ggraph.resolveBatch(ctx, batch, o, true)
			}
			return nil
		})
	}()
	return o
}

// GetInEdgeChannel process requests of vertex ids and find the connected incoming edges
func (ggraph *Graph) GetInEdgeChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
	edgeLabelSet := make(map[string]struct{}, len(edgeLabels))
	for _, label := range edgeLabels {
		edgeLabelSet[label] = struct{}{}
	}
	go func() {
		defer close(o)
		ggraph.driver.Pkv.View(func(it *pebblebulk.PebbleIterator) error {
			var batch []gdbi.ElementLookup
			for req := range reqChan {
				if req.IsSignal() {
					if len(batch) > 0 {
						ggraph.resolveBatch(ctx, batch, o, true)
						batch = nil
					}
					o <- req
					continue
				}
				found := false
				uid, ok := lookupUIDFromRequest(req, ggraph)
				if !ok {
					if emitNull {
						req.Edge = nil
						o <- req
					}
					continue
				}
				dkeyPrefix := key.DstEdgePrefix(uid)
				for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
					euid, suid, duid, label := key.DstEdgeKeyParse(it.Key())
					if edgeLabelAllowed(edgeLabelSet, label) {
						if !load {
							eid, _ := ggraph.driver.TranslateID(euid)
							src, _ := ggraph.driver.TranslateID(suid)
							dst, _ := ggraph.driver.TranslateID(duid)
							e := gdbi.Edge{
								From:  src,
								To:    dst,
								Label: label,
								ID:    eid,
							}
							e.Data = map[string]any{}
							req.Edge = &e
							o <- req
						} else {
							e := gdbi.Edge{Label: label}
							byteVal, _ := it.Value()
							_, loc, data := benchtop.DecodeEdgeValue(byteVal)
							if data != nil {
								e.Data = data
								e.Loaded = true
							}
							batch = append(batch, gdbi.ElementLookup{
								Ref:  req.Ref,
								Edge: &e,
								Meta: gdbi.LookupMeta{Opaque: loc, Data: e.Data, EUID: euid, SUID: suid, DUID: duid},
							})
							if len(batch) >= 1000 {
								ggraph.resolveBatch(ctx, batch, o, true)
								batch = nil
							}
						}
						found = true
					}
				}

				if !found && emitNull {
					req.Edge = nil
					o <- req
				}
			}
			if len(batch) > 0 {
				ggraph.resolveBatch(ctx, batch, o, true)
			}
			return nil
		})
	}()
	return o
}
