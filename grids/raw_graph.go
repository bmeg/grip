package grids

import (
	"bytes"
	"context"

	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/util/setcmp"
)

func (ggraph *Graph) RawGetVertexList(ctx context.Context) <-chan *GRIDDataElement {
	o := make(chan *GRIDDataElement, 100)
	go func() {
		defer close(o)
		ggraph.graphkv.View(func(it kvi.KVIterator) error {
			vPrefix := VertexListPrefix()
			for it.Seek(vPrefix); it.Valid() && bytes.HasPrefix(it.Key(), vPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Key()
				vkey := VertexKeyParse(keyValue)
				lkey := ggraph.keyMap.GetVertexLabel(vkey)
				o <- &GRIDDataElement{
					Gid:    vkey,
					Label:  lkey,
					Data:   map[string]interface{}{},
					Loaded: false,
				}
			}
			return nil
		})
	}()
	return o
}

func (ggraph *Graph) RawGetEdgeList(ctx context.Context) <-chan *GRIDDataElement {
	o := make(chan *GRIDDataElement, 100)
	go func() {
		defer close(o)
		ggraph.graphkv.View(func(it kvi.KVIterator) error {
			ePrefix := EdgeListPrefix()
			for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Key()
				ekey, srcvkey, dstvkey, lkey := EdgeKeyParse(keyValue)
				o <- &GRIDDataElement{
					Gid:    ekey,
					Label:  lkey,
					From:   srcvkey,
					To:     dstvkey,
					Data:   map[string]interface{}{},
					Loaded: false,
				}
			}
			return nil
		})
	}()
	return o
}

func (ggraph *Graph) RawGetVertexChannel(reqChan chan *RawElementLookup) <-chan *RawElementLookup {
	o := make(chan *RawElementLookup, 100)
	go func() {
		defer close(o)
		for req := range reqChan {
			if req.IsSignal() {
				o <- req
			} else {
				vkey := req.ID
				lkey := ggraph.keyMap.GetVertexLabel(vkey)
				o <- &RawElementLookup{
					Element: &GRIDDataElement{
						Gid:    vkey,
						Label:  lkey,
						Data:   map[string]interface{}{},
						Loaded: false,
					},
					ID:  req.ID,
					Ref: req.Ref,
				}
			}
		}
	}()
	return o
}

func (ggraph *Graph) RawGetOutChannel(reqChan chan *RawElementLookup, edgeLabels []string) chan *RawElementLookup {
	o := make(chan *RawElementLookup, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.keyMap.GetLabelKey(edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					ePrefix := SrcEdgePrefix(req.ID)
					for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
						keyValue := it.Key()
						_, _, dstvkey, lkey := SrcEdgeKeyParse(keyValue)
						if len(edgeLabels) == 0 || setcmp.ContainsUint(edgeLabelKeys, lkey) {
							dstlkey := ggraph.keyMap.GetVertexLabel(dstvkey)
							o <- &RawElementLookup{
								Element: &GRIDDataElement{
									Gid:    dstvkey,
									Label:  dstlkey,
									Data:   map[string]interface{}{},
									Loaded: false,
								},
								ID:  req.ID,
								Ref: req.Ref,
							}
						}
					}
				}
			}
			return nil
		})
	}()
	return o
}

func (ggraph *Graph) RawGetInChannel(reqChan chan *RawElementLookup, edgeLabels []string) chan *RawElementLookup {
	o := make(chan *RawElementLookup, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.keyMap.GetLabelKey(edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					ePrefix := DstEdgePrefix(req.ID)
					for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
						keyValue := it.Key()
						_, srcvkey, _, lkey := DstEdgeKeyParse(keyValue)
						if len(edgeLabels) == 0 || setcmp.ContainsUint(edgeLabelKeys, lkey) {
							srclkey := ggraph.keyMap.GetVertexLabel(srcvkey)
							o <- &RawElementLookup{
								Element: &GRIDDataElement{
									Gid:    srcvkey,
									Label:  srclkey,
									Data:   map[string]interface{}{},
									Loaded: false,
								},
								ID:  req.ID,
								Ref: req.Ref,
							}
						}
					}
				}
			}
			return nil
		})
	}()
	return o
}

func (ggraph *Graph) RawGetOutEdgeChannel(reqChan chan *RawElementLookup, edgeLabels []string) chan *RawElementLookup {
	o := make(chan *RawElementLookup, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.keyMap.GetLabelKey(edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					ePrefix := SrcEdgePrefix(req.ID)
					for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
						keyValue := it.Key()
						ekey, srcvkey, dstvkey, lkey := SrcEdgeKeyParse(keyValue)
						if len(edgeLabels) == 0 || setcmp.ContainsUint(edgeLabelKeys, lkey) {
							o <- &RawElementLookup{
								Element: &GRIDDataElement{
									Gid:    ekey,
									Label:  lkey,
									From:   srcvkey,
									To:     dstvkey,
									Data:   map[string]interface{}{},
									Loaded: false,
								},
								ID:  req.ID,
								Ref: req.Ref,
							}
						}
					}
				}
			}
			return nil
		})
	}()
	return o
}

func (ggraph *Graph) RawGetInEdgeChannel(reqChan chan *RawElementLookup, edgeLabels []string) chan *RawElementLookup {
	o := make(chan *RawElementLookup, 100)
	edgeLabelKeys := make([]uint64, 0, len(edgeLabels))
	for i := range edgeLabels {
		el, ok := ggraph.keyMap.GetLabelKey(edgeLabels[i])
		if ok {
			edgeLabelKeys = append(edgeLabelKeys, el)
		}
	}
	go func() {
		defer close(o)
		ggraph.graphkv.View(func(it kvi.KVIterator) error {
			for req := range reqChan {
				if req.IsSignal() {
					o <- req
				} else {
					ePrefix := DstEdgePrefix(req.ID)
					for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Key(), ePrefix); it.Next() {
						keyValue := it.Key()
						ekey, srcvkey, dstvkey, lkey := DstEdgeKeyParse(keyValue)
						if len(edgeLabels) == 0 || setcmp.ContainsUint(edgeLabelKeys, lkey) {
							o <- &RawElementLookup{
								Element: &GRIDDataElement{
									Gid:    ekey,
									Label:  lkey,
									From:   srcvkey,
									To:     dstvkey,
									Data:   map[string]interface{}{},
									Loaded: false,
								},
								ID:  req.ID,
								Ref: req.Ref,
							}
						}
					}
				}
			}
			return nil
		})
	}()
	return o
}
