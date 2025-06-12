package grids

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/cockroachdb/pebble"

	"github.com/bmeg/grip/log"

	ristretto "github.com/dgraph-io/ristretto/v2"
)

type GetSet interface {
	Get(key []byte) ([]byte, io.Closer, error)
	Set(key, value []byte, _ *pebble.WriteOptions) error
	Delete(key []byte, _ *pebble.WriteOptions) error
}

type KeyMap struct {
	cache ristretto.Cache[string, uint64]

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

func NewKeyMap() *KeyMap {
	return &KeyMap{}
}

func (km *KeyMap) Close() {}

// GetsertVertexKey : Get or Insert Vertex Key
func (km *KeyMap) GetsertVertexKeyLabel(id, label string, db GetSet) (uint64, uint64) {
	o, ok := getIDKey(vIDPrefix, id, db)
	if !ok {
		km.vIncMut.Lock()
		var err error
		o, err = dbInc(&km.vIncCur, vInc, db)
		if err != nil {
			log.Errorf("%s", err)
		}
		km.vIncMut.Unlock()
		err = setKeyID(vKeyPrefix, id, o, db)
		if err != nil {
			log.Errorf("%s", err)
		}
		err = setIDKey(vIDPrefix, id, o, db)
		if err != nil {
			log.Errorf("%s", err)
		}
	}
	lkey := km.GetsertLabelKey(label, db)
	setIDLabel(vLabelPrefix, o, lkey, db)
	return o, lkey
}

func (km *KeyMap) GetsertVertexKey(id string, db GetSet) uint64 {
	o, ok := getIDKey(vIDPrefix, id, db)
	if !ok {
		km.vIncMut.Lock()
		var err error
		o, err = dbInc(&km.vIncCur, vInc, db)
		if err != nil {
			log.Errorf("%s", err)
		}
		km.vIncMut.Unlock()
		err = setKeyID(vKeyPrefix, id, o, db)
		if err != nil {
			log.Errorf("%s", err)
		}
		err = setIDKey(vIDPrefix, id, o, db)
		if err != nil {
			log.Errorf("%s", err)
		}
	}
	return o
}

func (km *KeyMap) GetVertexKey(id string, db GetSet) (uint64, bool) {
	return getIDKey(vIDPrefix, id, db)
}

// GetVertexID
func (km *KeyMap) GetVertexID(key uint64, db GetSet) (string, bool) {
	return getKeyID(vKeyPrefix, key, db)
}

func (km *KeyMap) GetVertexLabel(key uint64, db GetSet) uint64 {
	k, _ := getIDLabel(vLabelPrefix, key, db)
	return k
}

// GetsertEdgeKey gets or inserts a new uint64 id for a given edge GID string
func (km *KeyMap) GetsertEdgeKey(id, label string, db GetSet) (uint64, uint64) {
	o, ok := getIDKey(eIDPrefix, id, db)
	if !ok {
		km.eIncMut.Lock()
		o, _ = dbInc(&km.eIncCur, eInc, db)
		km.eIncMut.Unlock()
		if err := setKeyID(eKeyPrefix, id, o, db); err != nil {
			log.Errorf("%s", err)
		}
		if err := setIDKey(eIDPrefix, id, o, db); err != nil {
			log.Errorf("%s", err)
		}
	}
	lkey := km.GetsertLabelKey(label, db)
	if err := setIDLabel(eLabelPrefix, o, lkey, db); err != nil {
		log.Errorf("%s", err)
	}
	return o, lkey
}

// GetEdgeKey gets the uint64 key for a given GID string
func (km *KeyMap) GetEdgeKey(id string, db GetSet) (uint64, bool) {
	return getIDKey(eIDPrefix, id, db)
}

// GetEdgeID gets the GID string for a given edge id uint64
func (km *KeyMap) GetEdgeID(key uint64, db GetSet) (string, bool) {
	return getKeyID(eKeyPrefix, key, db)
}

func (km *KeyMap) GetEdgeLabel(key uint64, db GetSet) uint64 {
	k, _ := getIDLabel(eLabelPrefix, key, db)
	return k
}

// DelVertexKey
func (km *KeyMap) DelVertexKey(id string, db GetSet) error {
	key, ok := km.GetVertexKey(id, db)
	if !ok {
		return fmt.Errorf("%s vertexKey not found", id)
	}
	if err := delKeyID(vKeyPrefix, key, db); err != nil {
		return err
	}
	if err := delIDKey(vIDPrefix, id, db); err != nil {
		return err
	}
	return nil
}

// DelEdgeKey
func (km *KeyMap) DelEdgeKey(id string, db GetSet) error {
	key, ok := km.GetEdgeKey(id, db)
	if !ok {
		return fmt.Errorf("%s edgeKey not found", id)
	}
	if err := delKeyID(eKeyPrefix, key, db); err != nil {
		return err
	}
	if err := delIDKey(eIDPrefix, id, db); err != nil {
		return err
	}
	return nil
}

// GetsertLabelKey gets-or-inserts a new label key uint64 for a given string
func (km *KeyMap) GetsertLabelKey(id string, db GetSet) uint64 {
	u, ok := getIDKey(lIDPrefix, id, db)
	if ok {
		return u
	}
	km.lIncMut.Lock()
	o, _ := dbInc(&km.lIncCur, lInc, db)
	km.lIncMut.Unlock()
	if err := setKeyID(lKeyPrefix, id, o, db); err != nil {
		log.Errorf("%s", err)
	}
	if err := setIDKey(lIDPrefix, id, o, db); err != nil {
		log.Errorf("%s", err)
	}
	return o
}

func (km *KeyMap) GetLabelKey(id string, db GetSet) (uint64, bool) {
	return getIDKey(lIDPrefix, id, db)
}

// GetLabelID gets the GID for a given uint64 label key
func (km *KeyMap) GetLabelID(key uint64, db GetSet) (string, bool) {
	return getKeyID(lKeyPrefix, key, db)
}

func getIDKey(prefix []byte, id string, db GetSet) (uint64, bool) {
	v, closer, err := db.Get(bytes.Join([][]byte{prefix, []byte(id)}, []byte{}))
	if v == nil || err != nil {
		return 0, false
	}
	key, _ := binary.Uvarint(v)
	closer.Close()
	return key, true
}

func setIDKey(prefix []byte, id string, key uint64, db GetSet) error {
	k := bytes.Join([][]byte{prefix, []byte(id)}, []byte{})
	b := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(b, key)
	return db.Set(k, b, nil)
}

func delIDKey(prefix []byte, id string, db GetSet) error {
	k := bytes.Join([][]byte{prefix, []byte(id)}, []byte{})
	return db.Delete(k, nil)
}

func getIDLabel(prefix byte, key uint64, db GetSet) (uint64, bool) {
	k := make([]byte, 1+binary.MaxVarintLen64)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], key)
	v, closer, err := db.Get(k)
	if v == nil || err != nil {
		return 0, false
	}
	label, _ := binary.Uvarint(v)
	closer.Close()
	return label, true
}

func setIDLabel(prefix byte, key uint64, label uint64, db GetSet) error {
	k := make([]byte, binary.MaxVarintLen64+1)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], key)

	b := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(b, label)

	err := db.Set(k, b, nil)
	return err
}

func setKeyID(prefix byte, id string, key uint64, db GetSet) error {
	k := make([]byte, binary.MaxVarintLen64+1)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], key)
	return db.Set(k, []byte(id), nil)
}

func getKeyID(prefix byte, key uint64, db GetSet) (string, bool) {
	k := make([]byte, binary.MaxVarintLen64+1)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], key)
	b, closer, err := db.Get(k)
	if b == nil || err != nil {
		return "", false
	}
	out := string(b)
	closer.Close()
	return out, true
}

func delKeyID(prefix byte, key uint64, db GetSet) error {
	k := make([]byte, binary.MaxVarintLen64+1)
	k[0] = prefix
	binary.PutUvarint(k[1:binary.MaxVarintLen64+1], key)
	return db.Delete(k, nil)
}

func dbInc(inc *uint64, k []byte, db GetSet) (uint64, error) {
	b := make([]byte, binary.MaxVarintLen64)
	if *inc == 0 {
		v, closer, _ := db.Get(k)
		if v == nil {
			binary.PutUvarint(b, incMod)
			if err := db.Set(k, b, nil); err != nil {
				return 0, err
			}
			(*inc) += 2
			return 1, nil
		}
		closer.Close()
		newInc, _ := binary.Uvarint(v)
		*inc = newInc
		binary.PutUvarint(b, (*inc)+incMod)
		if err := db.Set(k, b, nil); err != nil {
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
		if err := db.Set(k, b, nil); err != nil {
			return 0, err
		}
	}
	return o, nil
}
