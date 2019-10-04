package grids

import (
  "sync"
  "bytes"
  "encoding/binary"
  "github.com/akrylysov/pogreb"

)

type KeyMap struct {
  db      *pogreb.DB

  gIncCur  uint64
  vIncCur  uint64
  eIncCur  uint64
  lIncCur  uint64

  gIncMut  sync.Mutex
  vIncMut  sync.Mutex
  eIncMut  sync.Mutex
  lIncMut  sync.Mutex
}

var incMod uint64 = 1000

var gIdPrefix []byte = []byte{'g'}
var vIdPrefix []byte = []byte{'v'}
var eIdPrefix []byte = []byte{'e'}
var lIdPrefix []byte = []byte{'l'}

var gKeyPrefix byte = 'G'
var vKeyPrefix byte = 'V'
var eKeyPrefix byte = 'E'
var lKeyPrefix byte = 'L'

var vLabelPrefix byte = 'x'
var eLabelPrefix byte = 'y'

var gInc []byte = []byte{'i', 'g'}
var vInc []byte = []byte{'i', 'v'}
var eInc []byte = []byte{'i', 'e'}
var lInc []byte = []byte{'i', 'l'}


func NewKeyMap(kv *pogreb.DB) *KeyMap {
  return &KeyMap{db:kv}
}


func (km *KeyMap) Close() {
  km.db.Close()
}


//TODO: implement
func (km *KeyMap) GetGraphKey(id string) uint64 {
  u, ok := getIdKey(gIdPrefix, id, km.db)
  if ok {
    return u
  }
  km.gIncMut.Lock()
  o := dbInc(&km.gIncCur, gInc, km.db)
  km.gIncMut.Unlock()
  setKeyId(gKeyPrefix, id, o, km.db)
  setIdKey(gIdPrefix, id, o, km.db)
  return o
}

//GetGraphID
func (km *KeyMap) GetGraphID(key uint64) string {
  k, _ := getKeyId(gKeyPrefix, key, km.db)
  return k
}

//GetsertVertexKey : Get or Insert Vertex Key
func (km *KeyMap) GetsertVertexKey(id, label string) (uint64, uint64) {
  o, ok := getIdKey(vIdPrefix, id, km.db)
  if !ok {
    km.vIncMut.Lock()
    o = dbInc(&km.vIncCur, vInc, km.db)
    km.vIncMut.Unlock()
    setKeyId(vKeyPrefix, id, o, km.db)
    setIdKey(vIdPrefix, id, o, km.db)
  }
  lkey := km.GetsertLabelKey(label)
  return o, lkey
}

func (km *KeyMap) GetVertexKey(id string) (uint64, bool) {
  return getIdKey(vIdPrefix, id, km.db)
}

//GetVertexID
func (km *KeyMap) GetVertexID(key uint64) string {
  k, _ := getKeyId(vKeyPrefix, key, km.db)
  return k
}

func (km *KeyMap) GetVertexLabel(key uint64) uint64 {
  k, _ := getIdLabel(vLabelPrefix, key, km.db)
  return k
}

/*
func (km *KeyMap) SetVertexLabel(key, label uint64) {
  setIdLabel(vLabelPrefix, key, label, km.db)
}
*/

func (km *KeyMap) DelVertexLabel(key uint64) {

}

//TODO: implement
func (km *KeyMap) GetsertEdgeKey(id, label string) (uint64, uint64) {
  o, ok := getIdKey(eIdPrefix, id, km.db)
  if !ok {
    km.eIncMut.Lock()
    o = dbInc(&km.eIncCur, eInc, km.db)
    km.eIncMut.Unlock()
    setKeyId(eKeyPrefix, id, o, km.db)
    setIdKey(eIdPrefix, id, o, km.db)
  }
  lkey := km.GetsertLabelKey(label)
  return o, lkey
}

//TODO: implement
func (km *KeyMap) GetEdgeKey(id string) (uint64, bool) {
  return getIdKey(eIdPrefix, id, km.db)
}


//TODO: implement
func (km *KeyMap) GetEdgeID(key uint64) string {
  k, _ := getKeyId(eKeyPrefix, key, km.db)
  return k
}

func (km *KeyMap) GetEdgeLabel(key uint64) uint64 {
  k, _ := getIdLabel(eLabelPrefix, key, km.db)
  return k
}

func (km *KeyMap) SetEdgeLabel(key, label uint64) {
  setIdLabel(eLabelPrefix, key, label, km.db)
}

func (km *KeyMap) DelEdgeLabel(key uint64) {

}

//TODO: implement
func (km *KeyMap) DelEdgeKey(id string) {
}


//TODO: implement
func (km *KeyMap) GetsertLabelKey(id string) uint64 {
  u, ok := getIdKey(lIdPrefix, id, km.db)
  if ok {
    return u
  }
  km.lIncMut.Lock()
  o := dbInc(&km.lIncCur, lInc, km.db)
  km.lIncMut.Unlock()
  setKeyId(lKeyPrefix, id, o, km.db)
  setIdKey(lIdPrefix, id, o, km.db)
  return o
}

func (km *KeyMap) GetLabelKey(id string) (uint64, bool) {
  return getIdKey(lIdPrefix, id, km.db)
}

//TODO: implement
func (km *KeyMap) GetLabelID(key uint64) string {
  k, _ := getKeyId(lKeyPrefix, key, km.db)
  return k
}


func getIdKey(prefix []byte, id string, db *pogreb.DB) (uint64, bool) {
  k := bytes.Join( [][]byte{ prefix, []byte(id) }, []byte{} )
  v, err := db.Get(k)
  if v == nil || err != nil {
    return 0, false
  }
  key, _ := binary.Uvarint(v)
  return key, true
}

func getIdLabel(prefix byte, key uint64, db *pogreb.DB) (uint64, bool) {
  k := make([]byte, binary.MaxVarintLen64 + 1)
  k[0] = prefix
  binary.PutUvarint(k, key)
  v, err := db.Get(k)
  if v == nil || err != nil {
    return 0, false
  }
  label, _ := binary.Uvarint(v)
  return label, true
}

func setIdLabel(prefix byte, key uint64, label uint64, db *pogreb.DB) error {
  k := make([]byte, binary.MaxVarintLen64 + 1)
  k[0] = prefix
  binary.PutUvarint(k, key)

  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, label)

  err := db.Put(k, b)
  return err
}

func setIdKey(prefix []byte, id string, key uint64, db *pogreb.DB) {
  k := bytes.Join( [][]byte{ prefix, []byte(id) }, []byte{} )
  b := make([]byte, binary.MaxVarintLen64)
  binary.PutUvarint(b, key)
  db.Put(k, b)
}

func setKeyId(prefix byte, id string, key uint64, db *pogreb.DB) {
  k := make([]byte, binary.MaxVarintLen64 + 1)
  k[0] = prefix
  binary.PutUvarint(k[1:binary.MaxVarintLen64+1], key)
  db.Put(k, []byte(id))
}

func getKeyId(prefix byte, key uint64, db *pogreb.DB) (string, bool) {
  k := make([]byte, binary.MaxVarintLen64 + 1)
  k[0] = prefix
  binary.PutUvarint(k[1:binary.MaxVarintLen64+1], key)
  b, err := db.Get(k)
  if b == nil || err != nil {
    return "", false
  }
  return string(b), true
}


func dbInc(inc *uint64, k []byte, db *pogreb.DB) uint64 {
  b := make([]byte, binary.MaxVarintLen64)
  if *inc == 0 {
    v, _ := db.Get(k)
    if v == nil {
      binary.PutUvarint(b, incMod)
      db.Put(gInc, b)
      (*inc)++
      return 0
    } else {
      newInc, _ := binary.Uvarint(v)
      *inc = newInc
      binary.PutUvarint(b, (*inc) + incMod)
      db.Put(k, b)
      o := (*inc)
      (*inc)++
      return o
    }
  }
  o := *inc
  (*inc)++
  if *inc % incMod == 0 {
    binary.PutUvarint(b, *inc + incMod)
    db.Put(gInc, b)
  }
  return o
}
