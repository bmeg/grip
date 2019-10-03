package grids

import (
  "bytes"
  "context"
  "github.com/bmeg/grip/gdbi"
  "github.com/bmeg/grip/kvi"
)

type RawDataElement struct {
  Gid   uint64
  To    uint64
  From  uint64
  Label uint64
}

// ElementLookup request to look up data
type RawElementLookup struct {
	ID     uint64
	Ref    interface{}
	Data   *RawDataElement
}

func (rd *RawDataElement) VertexDataElement(ggraph *GridsGraph) *gdbi.DataElement {
  Gid := ggraph.kdb.keyMap.GetVertexID(rd.Gid)
  Label := ggraph.kdb.keyMap.GetLabelID(rd.Label)
  return &gdbi.DataElement{ID:Gid, Label:Label}
}

func (rd *RawDataElement) EdgeDataElement(ggraph *GridsGraph) *gdbi.DataElement {
  Gid := ggraph.kdb.keyMap.GetEdgeID(rd.Gid)
  Label := ggraph.kdb.keyMap.GetLabelID(rd.Label)
  To := ggraph.kdb.keyMap.GetEdgeID(rd.To)
  From := ggraph.kdb.keyMap.GetEdgeID(rd.From)
  return &gdbi.DataElement{ID:Gid, To:To, From:From, Label:Label}
}

func (ggraph *GridsGraph) RawGetVertexList(ctx context.Context) <-chan *RawDataElement {
  o := make(chan *RawDataElement, 100)
  go func() {
    defer close(o)
    ggraph.kdb.graphkv.View(func(it kvi.KVIterator) error {
      vPrefix := VertexListPrefix(ggraph.graphKey)
      for it.Seek(vPrefix); it.Valid() && bytes.HasPrefix(it.Key(), vPrefix); it.Next() {
        select {
        case <-ctx.Done():
          return nil
        default:
        }
        keyValue := it.Key()
        _, vid := VertexKeyParse(keyValue)

        o <- &RawDataElement{
          Gid: vid,
        }

      }
      return nil
    })
  }()
  return o
}

func (ggraph *GridsGraph) RawGetEdgeList(ctx context.Context) <-chan *RawDataElement {
  //TODO:
  return nil
}

func (ggraph *GridsGraph) RawGetOutChannel(reqChan chan *RawElementLookup, load bool, edgeLabels []string) chan *RawElementLookup {
  //TODO:
  return nil
}

func (ggraph *GridsGraph) RawGetInChannel(reqChan chan *RawElementLookup, load bool, edgeLabels []string) chan *RawElementLookup {
  //TODO:
  return nil
}

func (ggraph *GridsGraph) RawGetOutEdgeChannel(reqChan chan *RawElementLookup, load bool, edgeLabels []string) chan *RawElementLookup {
  //TODO:
  return nil
}

func (ggraph *GridsGraph) RawGetInEdgeChannel(reqChan chan *RawElementLookup, load bool, edgeLabels []string) chan *RawElementLookup {
  //TODO:
  return nil
}
