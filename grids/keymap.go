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

	vIncCur uint64
	eIncCur uint64
	lIncCur uint64

	vIncMut sync.Mutex
	eIncMut sync.Mutex
	lIncMut sync.Mutex
}

var incMod uint64 = 1000

var vIDPrefix = []byte{'v'}
var eIDPrefix = []byte{'e'}
var lIDPrefix = []byte{'l'}

var vKeyPrefix byte = 'V'
var eKeyPrefix byte = 'E'
var lKeyPrefix byte = 'L'

var vLabelPrefix byte = 'x'
var eLabelPrefix byte = 'y'

var vInc = []byte{'i', 'v'}
var eInc = []byte{'i', 'e'}
var lInc = []byte{'i', 'l'}

func NewKeyMap(kv *pogreb.DB) *KeyMap {
	return &KeyMap{db: kv}
}

func (km *KeyMap) Close() {
	km.db.Close()
}

//GetsertVertexKey : Get or Insert Vertex Key
func (km *KeyMap) GetsertVertexKey(id, label string) (uint64, uint64) {
	o, ok := getIDKey(vIDPrefix, id, km.db)
	if !ok {
		km.vIncMut.Lock()
		var err error
		o, err = dbInc(&km.vIncCur, vInc, km.db)
		if err != nil {
			log.Errorf("%s", err)
		}
		km.vIncMut.Unlock()
		err = setKeyID(vKeyPrefix, id, o, km.db)
		if err != nil {
			log.Errorf("%s", err)
		}
		err = setIDKey(vIDPrefix, id, o, km.db)
		if err != nil {
			log.Errorf("%s", err)
		}
	}
	lkey := km.GetsertLabelKey(label)
	setIDLabel(vLabelPrefix, o, lkey, km.db)
	return o, lkey
}

func (km *KeyMap) GetVertexKey(id string) (uint64, bool) {
	return getIDKey(vIDPrefix, id, km.db)
}

//GetVertexID
func (km *KeyMap) GetVertexID(key uint64) (string, bool) {
	return getKeyID(vKeyPrefix, key, km.db)
}

func (km *KeyMap) GetVertexLabel(key uint64) uint64 {
	k, _ := getIDLabel(vLabelPrefix, key, km.db)
	return k
}

//GetsertEdgeKey gets or inserts a new uint64 id for a given edge GID string
func (km *KeyMap) GetsertEdgeKey(id, label string) (uint64, uint64) {
	o, ok := getIDKey(eIDPrefix, id, km.db)
	if !ok {
		km.eIncMut.Lock()
		o, _ = dbInc(&km.eIncCur, eInc, km.db)
		km.eIncMut.Unlock()
		if err := setKeyID(eKeyPrefix, id, o, km.db); err != nil {
			log.Errorf("%s", err)
		}
		if err := setIDKey(eIDPrefix, id, o, km.db); err != nil {
			log.Errorf("%s", err)
		}
	}
	lkey := km.GetsertLabelKey(label)
	if err := setIDLabel(eLabelPrefix, o, lkey, km.db); err != nil {
		log.Errorf("%s", err)
	}
	return o, lkey
}

//GetEdgeKey gets the uint64 key for a given GID string
func (km *KeyMap) GetEdgeKey(id string) (uint64, bool) {
	return getIDKey(eIDPrefix, id, km.db)
}

//GetEdgeID gets the GID string for a given edge id uint64
func (km *KeyMap) GetEdgeID(key uint64) (string, bool) {
	return getKeyID(eKeyPrefix, key, km.db)
}

func (km *KeyMap) GetEdgeLabel(key uint64) uint64 {
	k, _ := getIDLabel(eLabelPrefix, key, km.db)
	return k
}

//DelVertexKey
func (km *KeyMap) DelVertexKey(id string) error {
	key, ok := km.GetVertexKey(id)
	if !ok {
		return fmt.Errorf("%s vertexKey not found", id)
	}
	if err := delKeyID(vKeyPrefix, key, km.db); err != nil {
		return err
	}
	if err := delIDKey(vIDPrefix, id, km.db); err != nil {
		return err
	}
	return nil
}

//DelEdgeKey
func (km *KeyMap) DelEdgeKey(id string) error {
	key, ok := km.GetEdgeKey(id)
	if !ok {
		return fmt.Errorf("%s edgeKey not found", id)
	}
	if err := delKeyID(eKeyPrefix, key, km.db); err != nil {
		return err
	}
	if err := delIDKey(eIDPrefix, id, km.db); err != nil {
		return err
	}
	return nil
}

//GetsertLabelKey gets-or-inserts a new label key uint64 for a given string
func (km *KeyMap) GetsertLabelKey(id string) uint64 {
	u, ok := getIDKey(lIDPrefix, id, km.db)
	if ok {
		return u
	}
	km.lIncMut.Lock()
	o, _ := dbInc(&km.lIncCur, lInc, km.db)
	km.lIncMut.Unlock()
	if err := setKeyID(lKeyPrefix, id, o, km.db); err != nil {
		log.Errorf("%s", err)
	}
	if err := setIDKey(lIDPrefix, id, o, km.db); err != nil {
		log.Errorf("%s", err)
	}
	return o
}

func (km *KeyMap) GetLabelKey(id string) (uint64, bool) {
	return getIDKey(lIDPrefix, id, km.db)
}

//GetLabelID gets the GID for a given uint64 label key
func (km *KeyMap) GetLabelID(key uint64) (string, bool) {
	return getKeyID(lKeyPrefix, key, km.db)
}

func getIDKey(prefix []byte, id string, db *pogreb.DB) (uint64, bool) {
	k := bytes.Join([][]byte{prefix, []byte(id)}, []byte{})
	v, err := db.Get(k)
	if v == nil || err != nil {
		return 0, false
	}
	key, _ := binary.Uvarint(v)
	return key, true
}

func setIDKey(prefix []byte, id string, key uint64, db *pogreb.DB) error {
	k := bytes.Join([][]byte{prefix, []byte(id)}, []byte{})
	b := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(b, key)
	return db.Put(k, b)
}

func delIDKey(prefix []byte, id string, db *pogreb.DB) error {
	k := bytes.Join([][]byte{prefix, []byte(id)}, []byte{})
	return db.Delete(k)
}

func getIDLabel(prefix byte, key uint64, db *pogreb.DB) (uint64, bool) {
	k := make([]byte, 1+binary.MaxVarintLen64)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], key)
	v, err := db.Get(k)
	if v == nil || err != nil {
		return 0, false
	}
	label, _ := binary.Uvarint(v)
	return label, true
}

func setIDLabel(prefix byte, key uint64, label uint64, db *pogreb.DB) error {
	k := make([]byte, binary.MaxVarintLen64+1)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], key)

	b := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(b, label)

	err := db.Put(k, b)
	return err
}

func setKeyID(prefix byte, id string, key uint64, db *pogreb.DB) error {
	k := make([]byte, binary.MaxVarintLen64+1)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], key)
	return db.Put(k, []byte(id))
}

func getKeyID(prefix byte, key uint64, db *pogreb.DB) (string, bool) {
	k := make([]byte, binary.MaxVarintLen64+1)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], key)
	b, err := db.Get(k)
	if b == nil || err != nil {
		return "", false
	}
	return string(b), true
}

func delKeyID(prefix byte, key uint64, db *pogreb.DB) error {
	k := make([]byte, binary.MaxVarintLen64+1)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], key)
	return db.Delete(k)
}

func dbInc(inc *uint64, k []byte, db *pogreb.DB) (uint64, error) {
	b := make([]byte, binary.MaxVarintLen64)
	if *inc == 0 {
		v, _ := db.Get(k)
		if v == nil {
			binary.PutUvarint(b, incMod)
			if err := db.Put(k, b); err != nil {
				return 0, err
			}
			(*inc) += 2
			return 1, nil
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
		if err := db.Put(k, b); err != nil {
			return 0, err
		}
	}
	return o, nil
}
