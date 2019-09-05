package grids

import (
  "github.com/akrylysov/pogreb"

)

type KeyMap struct {
  keykv    pogreb.DB
}

//TODO: implement
func (km *KeyMap) GetGraphKey(id string) uint64 {
  return 0
}

//TODO: implement
func (km *KeyMap) GetVertexKey(id string) uint64 {
  return 0
}

//TODO: implement
func (km *KeyMap) GetEdgeKey(id string) uint64 {
  return 0
}

//TODO: implement
func (km *KeyMap) DelEdgeKey(id string) {
}


//TODO: implement
func (km *KeyMap) GetLabelKey(id string) uint64 {
  return 0
}
