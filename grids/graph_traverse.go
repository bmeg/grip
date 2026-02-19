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
				skeyPrefix := key.SrcEdgePrefix(req.ID)
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					_, _, dst, label := key.SrcEdgeKeyParse(it.Key())
					if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
						if !load {
							req.Vertex = &gdbi.Vertex{ID: dst, Label: labelFromElementID(dst)}
							o <- req
						} else {
							batch = append(batch, gdbi.ElementLookup{ID: dst, Ref: req.Ref})
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
				dkeyPrefix := key.DstEdgePrefix(req.ID)
				for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
					_, sid, _, label := key.DstEdgeKeyParse(it.Key())
					if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
						if !load {
							req.Vertex = &gdbi.Vertex{ID: sid, Label: labelFromElementID(sid)}
							o <- req
						} else {
							batch = append(batch, gdbi.ElementLookup{ID: sid, Ref: req.Ref})
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
				skeyPrefix := key.SrcEdgePrefix(req.ID)
				for it.Seek(skeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), skeyPrefix); it.Next() {
					eid, src, dst, label := key.SrcEdgeKeyParse(it.Key())
					if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
						byteVal, _ := it.Value()
						_, loc := benchtop.DecodeEdgeValue(byteVal)
						e := gdbi.Edge{
							From:  src,
							To:    dst,
							Label: label,
							ID:    eid,
						}
						if !load {
							e.Data = map[string]any{}
							req.Edge = &e
							o <- req
						} else {
							batch = append(batch, gdbi.ElementLookup{ID: eid, Ref: req.Ref, Edge: &e, Priv: loc})
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
				dkeyPrefix := key.DstEdgePrefix(req.ID)
				for it.Seek(dkeyPrefix); it.Valid() && bytes.HasPrefix(it.Key(), dkeyPrefix); it.Next() {
					eid, src, dst, label := key.DstEdgeKeyParse(it.Key())
					if len(edgeLabels) == 0 || setcmp.ContainsString(edgeLabels, label) {
						byteVal, _ := it.Value()
						_, loc := benchtop.DecodeEdgeValue(byteVal)
						e := gdbi.Edge{
							From:  src,
							To:    dst,
							Label: label,
							ID:    eid,
						}
						if !load {
							e.Data = map[string]any{}
							req.Edge = &e
							o <- req
						} else {
							batch = append(batch, gdbi.ElementLookup{ID: eid, Ref: req.Ref, Edge: &e, Priv: loc})
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
