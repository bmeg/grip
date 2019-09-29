package grids

import (
  "context"
  "github.com/bmeg/grip/gdbi"
)

type RawDataElement struct {
  Gid   uint64
  To    uint64
  From  uint64
  Label uint64
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


func (ggraph *GridsGraph) RawGetEdgeList(ctx context.Context) <-chan *RawDataElement {
  //TODO:
  return nil
}

func (ggraph *GridsGraph) RawGetVertexList(ctx context.Context) <-chan *RawDataElement {
  //TODO:
  return nil
}
