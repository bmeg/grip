package grids

import (
  "github.com/akrylysov/pogreb"

)

type KeyMap struct {
  keykv    *pogreb.DB
}

func NewKeyMap(kv *pogreb.DB) KeyMap {
  return KeyMap{kv}
}


func (km *KeyMap) Close() {
  km.keykv.Close()
}


//TODO: implement
func (km *KeyMap) GetGraphKey(id string) uint64 {
  return 0
}

//TODO: implement
func (km *KeyMap) GetGraphID(key uint64) string {
  return ""
}

//TODO: implement
func (km *KeyMap) GetVertexKey(id string) uint64 {
  return 0
}

//TODO: implement
func (km *KeyMap) GetVertexID(key uint64) string {
  return "0"
}


//TODO: implement
func (km *KeyMap) GetEdgeKey(id string) uint64 {
  return 0
}

//TODO: implement
func (km *KeyMap) GetEdgeID(key uint64) string {
  return "0"
}


//TODO: implement
func (km *KeyMap) DelEdgeKey(id string) {
}


//TODO: implement
func (km *KeyMap) GetLabelKey(id string) uint64 {
  return 0
}


//TODO: implement
func (km *KeyMap) GetLabelID(key uint64) string {
  return "0"
}
