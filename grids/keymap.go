package grids

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/akrylysov/pogreb"
	"github.com/bmeg/grip/log"
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

func (km *KeyMap) GetGraphKey(id string) (uint64, error) {
	u, ok := getIDKey(0, gIDPrefix, id, km.db)
	if ok {
		return u, nil
	}
	km.gIncMut.Lock()
	o, err := dbInc(&km.gIncCur, gInc, km.db)
	km.gIncMut.Unlock()
	if err != nil {
		return o, err
	}
	if err := setKeyID(0, gKeyPrefix, id, o, km.db); err != nil {
		return o, err
	}
	if err := setIDKey(0, gIDPrefix, id, o, km.db); err != nil {
		return o, err
	}
	return o, nil
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
		var err error
		o, err = dbInc(&km.vIncCur, vInc, km.db)
		if err != nil {
			log.Errorf("%s", err)
		}
		km.vIncMut.Unlock()
		err = setKeyID(graph, vKeyPrefix, id, o, km.db)
		if err != nil {
			log.Errorf("%s", err)
		}
		err = setIDKey(graph, vIDPrefix, id, o, km.db)
		if err != nil {
			log.Errorf("%s", err)
		}
	}
	lkey := km.GetsertLabelKey(graph, label)
	setIDLabel(graph, vLabelPrefix, o, lkey, km.db)
	return o, lkey
}

func (km *KeyMap) GetVertexKey(graph uint64, id string) (uint64, bool) {
	return getIDKey(graph, vIDPrefix, id, km.db)
}

//GetVertexID
func (km *KeyMap) GetVertexID(graph uint64, key uint64) (string, bool) {
	return getKeyID(graph, vKeyPrefix, key, km.db)
}

func (km *KeyMap) GetVertexLabel(graph uint64, key uint64) uint64 {
	k, _ := getIDLabel(graph, vLabelPrefix, key, km.db)
	return k
}

//GetsertEdgeKey gets or inserts a new uint64 id for a given edge GID string
func (km *KeyMap) GetsertEdgeKey(graph uint64, id, label string) (uint64, uint64) {
	o, ok := getIDKey(graph, eIDPrefix, id, km.db)
	if !ok {
		km.eIncMut.Lock()
		o, _ = dbInc(&km.eIncCur, eInc, km.db)
		km.eIncMut.Unlock()
		if err := setKeyID(graph, eKeyPrefix, id, o, km.db); err != nil {
			log.Errorf("%s", err)
		}
		if err := setIDKey(graph, eIDPrefix, id, o, km.db); err != nil {
			log.Errorf("%s", err)
		}
	}
	lkey := km.GetsertLabelKey(graph, label)
	if err := setIDLabel(graph, eLabelPrefix, o, lkey, km.db); err != nil {
		log.Errorf("%s", err)
	}
	return o, lkey
}

//GetEdgeKey gets the uint64 key for a given GID string
func (km *KeyMap) GetEdgeKey(graph uint64, id string) (uint64, bool) {
	return getIDKey(graph, eIDPrefix, id, km.db)
}

//GetEdgeID gets the GID string for a given edge id uint64
func (km *KeyMap) GetEdgeID(graph uint64, key uint64) (string, bool) {
	return getKeyID(graph, eKeyPrefix, key, km.db)
}

func (km *KeyMap) GetEdgeLabel(graph uint64, key uint64) uint64 {
	k, _ := getIDLabel(graph, eLabelPrefix, key, km.db)
	return k
}

//DelVertexKey
func (km *KeyMap) DelVertexKey(graph uint64, id string) error {
	key, ok := km.GetVertexKey(graph, id)
	if !ok {
		return fmt.Errorf("%s vertexKey not found", id)
	}
	if err := delKeyID(graph, vKeyPrefix, key, km.db); err != nil {
		return err
	}
	if err := delIDKey(graph, vIDPrefix, id, km.db); err != nil {
		return err
	}
	return nil
}

//DelEdgeKey
func (km *KeyMap) DelEdgeKey(graph uint64, id string) error {
	key, ok := km.GetEdgeKey(graph, id)
	if !ok {
		return fmt.Errorf("%s edgeKey not found", id)
	}
	if err := delKeyID(graph, eKeyPrefix, key, km.db); err != nil {
		return err
	}
	if err := delIDKey(graph, eIDPrefix, id, km.db); err != nil {
		return err
	}
	return nil
}

//GetsertLabelKey gets-or-inserts a new label key uint64 for a given string
func (km *KeyMap) GetsertLabelKey(graph uint64, id string) uint64 {
	u, ok := getIDKey(graph, lIDPrefix, id, km.db)
	if ok {
		return u
	}
	km.lIncMut.Lock()
	o, _ := dbInc(&km.lIncCur, lInc, km.db)
	km.lIncMut.Unlock()
	if err := setKeyID(graph, lKeyPrefix, id, o, km.db); err != nil {
		log.Errorf("%s", err)
	}
	if err := setIDKey(graph, lIDPrefix, id, o, km.db); err != nil {
		log.Errorf("%s", err)
	}
	return o
}

func (km *KeyMap) GetLabelKey(graph uint64, id string) (uint64, bool) {
	return getIDKey(graph, lIDPrefix, id, km.db)
}

//GetLabelID gets the GID for a given uint64 label key
func (km *KeyMap) GetLabelID(graph uint64, key uint64) (string, bool) {
	return getKeyID(graph, lKeyPrefix, key, km.db)
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

func setIDKey(graph uint64, prefix []byte, id string, key uint64, db *pogreb.DB) error {
	g := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(g, graph)
	k := bytes.Join([][]byte{prefix, g, []byte(id)}, []byte{})
	b := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(b, key)
	return db.Put(k, b)
}

func delIDKey(graph uint64, prefix []byte, id string, db *pogreb.DB) error {
	g := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(g, graph)
	k := bytes.Join([][]byte{prefix, g, []byte(id)}, []byte{})
	return db.Delete(k)
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

func setKeyID(graph uint64, prefix byte, id string, key uint64, db *pogreb.DB) error {
	k := make([]byte, binary.MaxVarintLen64*2+1)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], graph)
	binary.PutUvarint(k[binary.MaxVarintLen64+1:binary.MaxVarintLen64*2+1], key)
	return db.Put(k, []byte(id))
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

func delKeyID(graph uint64, prefix byte, key uint64, db *pogreb.DB) error {
	k := make([]byte, binary.MaxVarintLen64*2+1)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], graph)
	binary.PutUvarint(k[binary.MaxVarintLen64+1:binary.MaxVarintLen64*2+1], key)
	return db.Delete(k)
}

func dbInc(inc *uint64, k []byte, db *pogreb.DB) (uint64, error) {
	b := make([]byte, binary.MaxVarintLen64)
	if *inc == 0 {
		v, _ := db.Get(k)
		if v == nil {
			binary.PutUvarint(b, incMod)
			if err := db.Put(gInc, b); err != nil {
				return 0, err
			}
			(*inc)++
			return 0, nil
		}
		newInc, _ := binary.Uvarint(v)
		*inc = newInc
		binary.PutUvarint(b, (*inc)+incMod)
		if err := db.Put(k, b); err != nil {
			return 0, err
		}
		o := (*inc)
		(*inc)++
		return o, nil
	}
	o := *inc
	(*inc)++
	if *inc%incMod == 0 {
		binary.PutUvarint(b, *inc+incMod)
		if err := db.Put(gInc, b); err != nil {
			return 0, err
		}
	}
	return o, nil
}
