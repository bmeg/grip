package grids

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/akrylysov/pogreb"
)

type KeyMap struct {
	db *pogreb.DB

	gIncCur uint64
	vIncCur uint64
	eIncCur uint64
	lIncCur uint64

	gIncMut sync.Mutex
	vIncMut sync.Mutex
	eIncMut sync.Mutex
	lIncMut sync.Mutex
}

var incMod uint64 = 1000

var gIDPrefix = []byte{'g'}
var vIDPrefix = []byte{'v'}
var eIDPrefix = []byte{'e'}
var lIDPrefix = []byte{'l'}

var gKeyPrefix byte = 'G'
var vKeyPrefix byte = 'V'
var eKeyPrefix byte = 'E'
var lKeyPrefix byte = 'L'

var vLabelPrefix byte = 'x'
var eLabelPrefix byte = 'y'

var gInc = []byte{'i', 'g'}
var vInc = []byte{'i', 'v'}
var eInc = []byte{'i', 'e'}
var lInc = []byte{'i', 'l'}

func NewKeyMap(kv *pogreb.DB) *KeyMap {
	return &KeyMap{db: kv}
}

func (km *KeyMap) Close() {
	km.db.Close()
}

func (km *KeyMap) GetGraphKey(id string) uint64 {
	u, ok := getIDKey(0, gIDPrefix, id, km.db)
	if ok {
		return u
	}
	km.gIncMut.Lock()
	o := dbInc(&km.gIncCur, gInc, km.db)
	km.gIncMut.Unlock()
	setKeyID(0, gKeyPrefix, id, o, km.db)
	setIDKey(0, gIDPrefix, id, o, km.db)
	return o
}

//GetGraphID
func (km *KeyMap) GetGraphID(key uint64) string {
	k, _ := getKeyID(0, gKeyPrefix, key, km.db)
	return k
}

//GetsertVertexKey : Get or Insert Vertex Key
func (km *KeyMap) GetsertVertexKey(graph uint64, id, label string) (uint64, uint64) {
	o, ok := getIDKey(graph, vIDPrefix, id, km.db)
	if !ok {
		km.vIncMut.Lock()
		o = dbInc(&km.vIncCur, vInc, km.db)
		km.vIncMut.Unlock()
		setKeyID(graph, vKeyPrefix, id, o, km.db)
		setIDKey(graph, vIDPrefix, id, o, km.db)
	}
	lkey := km.GetsertLabelKey(graph, label)
	setIDLabel(graph, vLabelPrefix, o, lkey, km.db)
	return o, lkey
}

func (km *KeyMap) GetVertexKey(graph uint64, id string) (uint64, bool) {
	return getIDKey(graph, vIDPrefix, id, km.db)
}

//GetVertexID
func (km *KeyMap) GetVertexID(graph uint64, key uint64) string {
	k, _ := getKeyID(graph, vKeyPrefix, key, km.db)
	return k
}

func (km *KeyMap) GetVertexLabel(graph uint64, key uint64) uint64 {
	k, _ := getIDLabel(graph, vLabelPrefix, key, km.db)
	return k
}

//TODO: implement
func (km *KeyMap) GetsertEdgeKey(graph uint64, id, label string) (uint64, uint64) {
	o, ok := getIDKey(graph, eIDPrefix, id, km.db)
	if !ok {
		km.eIncMut.Lock()
		o = dbInc(&km.eIncCur, eInc, km.db)
		km.eIncMut.Unlock()
		setKeyID(graph, eKeyPrefix, id, o, km.db)
		setIDKey(graph, eIDPrefix, id, o, km.db)
	}
	lkey := km.GetsertLabelKey(graph, label)
	setIDLabel(graph, eLabelPrefix, o, lkey, km.db)
	return o, lkey
}

//TODO: implement
func (km *KeyMap) GetEdgeKey(graph uint64, id string) (uint64, bool) {
	return getIDKey(graph, eIDPrefix, id, km.db)
}

//TODO: implement
func (km *KeyMap) GetEdgeID(graph uint64, key uint64) string {
	k, _ := getKeyID(graph, eKeyPrefix, key, km.db)
	return k
}

func (km *KeyMap) GetEdgeLabel(graph uint64, key uint64) uint64 {
	k, _ := getIDLabel(graph, eLabelPrefix, key, km.db)
	return k
}

/*
func (km *KeyMap) SetEdgeLabel(key, label uint64) {
  setIDLabel(eLabelPrefix, key, label, km.db)
}
*/

//TODO: implement
func (km *KeyMap) DelEdgeKey(graph uint64, id string) {
}

//TODO: implement
func (km *KeyMap) GetsertLabelKey(graph uint64, id string) uint64 {
	u, ok := getIDKey(graph, lIDPrefix, id, km.db)
	if ok {
		return u
	}
	km.lIncMut.Lock()
	o := dbInc(&km.lIncCur, lInc, km.db)
	km.lIncMut.Unlock()
	setKeyID(graph, lKeyPrefix, id, o, km.db)
	setIDKey(graph, lIDPrefix, id, o, km.db)
	return o
}

func (km *KeyMap) GetLabelKey(graph uint64, id string) (uint64, bool) {
	return getIDKey(graph, lIDPrefix, id, km.db)
}

//TODO: implement
func (km *KeyMap) GetLabelID(graph uint64, key uint64) string {
	k, _ := getKeyID(graph, lKeyPrefix, key, km.db)
	return k
}

func getIDKey(graph uint64, prefix []byte, id string, db *pogreb.DB) (uint64, bool) {
	g := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(g, graph)
	k := bytes.Join([][]byte{prefix, g, []byte(id)}, []byte{})
	v, err := db.Get(k)
	if v == nil || err != nil {
		return 0, false
	}
	key, _ := binary.Uvarint(v)
	return key, true
}

func setIDKey(graph uint64, prefix []byte, id string, key uint64, db *pogreb.DB) {
	g := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(g, graph)
	k := bytes.Join([][]byte{prefix, g, []byte(id)}, []byte{})
	b := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(b, key)
	db.Put(k, b)
}

func getIDLabel(graph uint64, prefix byte, key uint64, db *pogreb.DB) (uint64, bool) {
	k := make([]byte, binary.MaxVarintLen64*2+1)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], graph)
	binary.PutUvarint(k[binary.MaxVarintLen64+1:binary.MaxVarintLen64*2+1], key)
	v, err := db.Get(k)
	if v == nil || err != nil {
		return 0, false
	}
	label, _ := binary.Uvarint(v)
	return label, true
}

func setIDLabel(graph uint64, prefix byte, key uint64, label uint64, db *pogreb.DB) error {
	k := make([]byte, binary.MaxVarintLen64*2+1)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], graph)
	binary.PutUvarint(k[binary.MaxVarintLen64+1:binary.MaxVarintLen64*2+1], key)

	b := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(b, label)

	err := db.Put(k, b)
	return err
}

func setKeyID(graph uint64, prefix byte, id string, key uint64, db *pogreb.DB) {
	k := make([]byte, binary.MaxVarintLen64*2+1)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], graph)
	binary.PutUvarint(k[binary.MaxVarintLen64+1:binary.MaxVarintLen64*2+1], key)
	db.Put(k, []byte(id))
}

func getKeyID(graph uint64, prefix byte, key uint64, db *pogreb.DB) (string, bool) {
	k := make([]byte, binary.MaxVarintLen64*2+1)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], graph)
	binary.PutUvarint(k[binary.MaxVarintLen64+1:binary.MaxVarintLen64*2+1], key)
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
		}
		newInc, _ := binary.Uvarint(v)
		*inc = newInc
		binary.PutUvarint(b, (*inc)+incMod)
		db.Put(k, b)
		o := (*inc)
		(*inc)++
		return o
	}
	o := *inc
	(*inc)++
	if *inc%incMod == 0 {
		binary.PutUvarint(b, *inc+incMod)
		db.Put(gInc, b)
	}
	return o
}
