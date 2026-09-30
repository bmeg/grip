package grids

import (
	"bytes"
	"context"

	"github.com/bmeg/benchtop"
	"github.com/bmeg/benchtop/pebblebulk"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/grids/key"
	"github.com/bmeg/grip/util/setcmp"
)

// GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (ggraph *Graph) GetOutChannel(ctx context.Context, reqChan chan gdbi.ElementLookup, load bool, emitNull bool, edgeLabels []string) chan gdbi.ElementLookup {
	o := make(chan gdbi.ElementLookup, 100)
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
				uid, _ := ggraph.driver.GetID(req.ID)
				skeyPrefix := key.SrcEdgePrefix(uid)
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					_, _, duid, label := key.SrcEdgeKeyParse(it.Key())
					if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
						dst, _ := ggraph.driver.TranslateID(duid)
						if !load {
							req.Vertex = &gdbi.Vertex{ID: dst, Label: labelFromElementID(dst)}
							o <- req
						} else {
							batch = append(batch, gdbi.ElementLookup{ID: dst, Ref: req.Ref, Priv: lookupPriv{uid: duid}})
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
				uid, _ := ggraph.driver.GetID(req.ID)
				dkeyPrefix := key.DstEdgePrefix(uid)
				for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
					_, suid, _, label := key.DstEdgeKeyParse(it.Key())
					if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
						src, _ := ggraph.driver.TranslateID(suid)
						if !load {
							req.Vertex = &gdbi.Vertex{ID: src, Label: labelFromElementID(src)}
							o <- req
						} else {
							batch = append(batch, gdbi.ElementLookup{ID: src, Ref: req.Ref, Priv: lookupPriv{uid: suid}})
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
				uid, _ := ggraph.driver.GetID(req.ID)
				skeyPrefix := key.SrcEdgePrefix(uid)
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					euid, suid, duid, label := key.SrcEdgeKeyParse(it.Key())
					if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
						eid, _ := ggraph.driver.TranslateID(euid)
						src, _ := ggraph.driver.TranslateID(suid)
						dst, _ := ggraph.driver.TranslateID(duid)

						byteVal, _ := it.Value()
						_, loc, data := benchtop.DecodeEdgeValue(byteVal)
						e := gdbi.Edge{
							From:  src,
							To:    dst,
							Label: label,
							ID:    eid,
						}
						if data != nil {
							e.Data = data
							e.Loaded = true
						}
						if !load {
							if e.Data == nil {
								e.Data = map[string]any{}
							}
							req.Edge = &e
							o <- req
						} else {
							batch = append(batch, gdbi.ElementLookup{ID: eid, Ref: req.Ref, Edge: &e, Priv: lookupPriv{loc: loc, data: e.Data}})
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
				uid, _ := ggraph.driver.GetID(req.ID)
				dkeyPrefix := key.DstEdgePrefix(uid)
				for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
					euid, suid, duid, label := key.DstEdgeKeyParse(it.Key())
					if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
						eid, _ := ggraph.driver.TranslateID(euid)
						src, _ := ggraph.driver.TranslateID(suid)
						dst, _ := ggraph.driver.TranslateID(duid)

						byteVal, _ := it.Value()
						_, loc, data := benchtop.DecodeEdgeValue(byteVal)
						e := gdbi.Edge{
							From:  src,
							To:    dst,
							Label: label,
							ID:    eid,
						}
						if data != nil {
							e.Data = data
							e.Loaded = true
						}
						if !load {
							if e.Data == nil {
								e.Data = map[string]any{}
							}
							req.Edge = &e
							o <- req
						} else {
							batch = append(batch, gdbi.ElementLookup{ID: eid, Ref: req.Ref, Edge: &e, Priv: lookupPriv{loc: loc, data: e.Data}})
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
