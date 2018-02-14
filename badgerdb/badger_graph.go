package badgerdb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/timestamp"
	"github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	"log"
	"math/rand"
	"os"
)

var cGraphPrefix = []byte("g")
var cVertexPrefix = []byte("v")
var cEdgePrefix = []byte("e")
var cSrcEdgePrefix = []byte("s")
var cDestEdgePrefix = []byte("d")

var cEdgeSingle byte = 0x00
var cEdgeBundle byte = 0x01

// BadgerArachne implements the Arachne interface using badger
type BadgerArachne struct {
	kv *badger.DB
	ts *timestamp.Timestamp
}

//BadgerGDB represents interface to a single graph controlled by the badger driver
type BadgerGDB struct {
	kv    *badger.DB
	graph string
	ts    *timestamp.Timestamp
}

func hasKey(kv *badger.DB, key []byte) bool {
	out := false
	kv.View(func(tx *badger.Txn) error {
		_, err := tx.Get(key)
		if err == nil {
			out = true
		}
		return nil
	})
	return out
}

func contains(a []string, v string) bool {
	for _, i := range a {
		if i == v {
			return true
		}
	}
	return false
}

func graphPrefix() []byte {
	return cGraphPrefix
}

func graphKey(graph string) []byte {
	return bytes.Join([][]byte{cGraphPrefix, []byte(graph)}, []byte{0})
}

func graphKeyParse(key []byte) string {
	tmp := bytes.Split(key, []byte{0})
	graph := string(tmp[1])
	return graph
}

func edgeKey(graph, id, src, dst string) []byte {
	return bytes.Join([][]byte{cEdgePrefix, []byte(graph), []byte(id), []byte(src), []byte(dst)}, []byte{0})
}

func edgeKeyPrefix(graph, id string) []byte {
	return bytes.Join([][]byte{cEdgePrefix, []byte(graph), []byte(id)}, []byte{0})
}

func edgeKeyParse(key []byte) (string, string, string, string) {
	tmp := bytes.Split(key, []byte{0})
	graph := tmp[1]
	eid := tmp[2]
	sid := tmp[3]
	did := tmp[4]
	return string(graph), string(eid), string(sid), string(did)
}

func vertexListPrefix(graph string) []byte {
	return bytes.Join([][]byte{cVertexPrefix, []byte(graph)}, []byte{0})
}

func edgeListPrefix(graph string) []byte {
	return bytes.Join([][]byte{cEdgePrefix, []byte(graph)}, []byte{0})
}

func srcEdgeKey(graph, src, dst, eid, label string) []byte {
	return bytes.Join([][]byte{cSrcEdgePrefix, []byte(graph), []byte(src), []byte(dst), []byte(eid), []byte(label)}, []byte{0})
}

func dstEdgeKey(graph, src, dst, eid, label string) []byte {
	return bytes.Join([][]byte{cDestEdgePrefix, []byte(graph), []byte(dst), []byte(src), []byte(eid), []byte(label)}, []byte{0})
}

func srcEdgeKeyPrefix(graph, src, dst, eid string) []byte {
	return bytes.Join([][]byte{cSrcEdgePrefix, []byte(graph), []byte(src), []byte(dst), []byte(eid)}, []byte{0})
}

func dstEdgeKeyPrefix(graph, src, dst, eid string) []byte {
	return bytes.Join([][]byte{cDestEdgePrefix, []byte(graph), []byte(dst), []byte(src), []byte(eid)}, []byte{0})
}

func srcEdgeKeyParse(key []byte) (string, string, string, string, string) {
	tmp := bytes.Split(key, []byte{0})
	graph := tmp[1]
	src := tmp[2]
	dst := tmp[3]
	eid := tmp[4]
	label := tmp[5]
	return string(graph), string(src), string(dst), string(eid), string(label)
}

func dstEdgeKeyParse(key []byte) (string, string, string, string, string) {
	tmp := bytes.Split(key, []byte{0})
	graph := tmp[1]
	dst := tmp[2]
	src := tmp[3]
	eid := tmp[4]
	label := tmp[5]
	return string(graph), string(src), string(dst), string(eid), string(label)
}

func srcEdgeListPrefix(graph string) []byte {
	return bytes.Join([][]byte{cSrcEdgePrefix, []byte(graph)}, []byte{0})
}

func dstEdgeListPrefix(graph string) []byte {
	return bytes.Join([][]byte{cDestEdgePrefix, []byte(graph)}, []byte{0})
}

func srcEdgePrefix(graph, id string) []byte {
	return bytes.Join([][]byte{cSrcEdgePrefix, []byte(graph), []byte(id)}, []byte{0})
}

func dstEdgePrefix(graph, id string) []byte {
	return bytes.Join([][]byte{cDestEdgePrefix, []byte(graph), []byte(id)}, []byte{0})
}

func vertexKey(graph, id string) []byte {
	return bytes.Join([][]byte{cVertexPrefix, []byte(graph), []byte(id)}, []byte{0})
}

func vertexKeyParse(key []byte) (string, string) {
	tmp := bytes.Split(key, []byte{0})
	graph := tmp[1]
	vid := tmp[2]
	return string(graph), string(vid)
}

// NewBadgerArachne creates a new gdbi.ArachneInterface driver using the badger
// driver at `path`
func NewBadgerArachne(path string) gdbi.ArachneInterface {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		os.Mkdir(path, 0700)
	}

	opts := badger.Options{}
	opts = badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	kv, err := badger.Open(opts)
	if err != nil {
		log.Printf("Error: %s", err)
	}
	log.Printf("Starting BadgerDB")
	ts := timestamp.NewTimestamp()
	o := &BadgerArachne{kv: kv, ts: &ts}
	for _, i := range o.GetGraphs() {
		o.ts.Touch(i)
	}
	return o
}

// AddGraph creates a new graph named `graph`
func (ba *BadgerArachne) AddGraph(graph string) error {
	ba.kv.Update(func(tx *badger.Txn) error {
		tx.Set(graphKey(graph), []byte{})
		return nil
	})
	ba.ts.Touch(graph)
	return nil
}

func bytesCopy(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func (ba *BadgerArachne) prefixDelete(prefix []byte) {
	deleteBlockSize := 10000

	for found := true; found; {
		found = false
		wb := make([][]byte, 0, deleteBlockSize)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		ba.kv.Update(func(tx *badger.Txn) error {
			it := tx.NewIterator(opts)
			for it.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), prefix) && len(wb) < deleteBlockSize-1; it.Next() {
				wb = append(wb, bytesCopy(it.Item().Key()))
			}
			it.Close()
			for _, i := range wb {
				tx.Delete(i)
				found = true
			}
			return nil
		})
	}
}

// DeleteGraph deletes `graph`
func (ba *BadgerArachne) DeleteGraph(graph string) error {
	eprefix := edgeListPrefix(graph)
	ba.prefixDelete(eprefix)

	vprefix := vertexListPrefix(graph)
	ba.prefixDelete(vprefix)

	sprefix := srcEdgeListPrefix(graph)
	ba.prefixDelete(sprefix)

	dprefix := dstEdgeListPrefix(graph)
	ba.prefixDelete(dprefix)

	graphKey := graphKey(graph)
	ba.kv.Update(func(tx *badger.Txn) error {
		tx.Delete(graphKey)
		return nil
	})
	ba.ts.Touch(graph)
	return nil
}

// Graph obtains the gdbi.DBI for a particular graph
func (ba *BadgerArachne) Graph(graph string) gdbi.DBI {
	return &BadgerGDB{kv: ba.kv, graph: graph, ts: ba.ts}
}

// Query creates a QueryInterface for Graph graph
func (ba *BadgerArachne) Query(graph string) gdbi.QueryInterface {
	return ba.Graph(graph).Query()
}

// Close the connection
func (ba *BadgerArachne) Close() {
	ba.kv.Close()
}

// GetGraphs lists the graphs managed by this driver
func (ba *BadgerArachne) GetGraphs() []string {
	out := make([]string, 0, 100)
	gPrefix := graphPrefix()
	ba.kv.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(gPrefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), gPrefix); it.Next() {
			out = append(out, graphKeyParse(it.Item().Key()))
		}
		return nil
	})
	return out
}

// Query creates a QueryInterface for a particular Graph
func (bgdb *BadgerGDB) Query() gdbi.QueryInterface {
	return gdbi.NewPipeEngine(bgdb)
}

// GetTimestamp returns the update timestamp
func (bgdb *BadgerGDB) GetTimestamp() string {
	return bgdb.ts.Get(bgdb.graph)
}

// SetVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (bgdb *BadgerGDB) SetVertex(vertex aql.Vertex) error {
	d, _ := proto.Marshal(&vertex)
	k := vertexKey(bgdb.graph, vertex.Gid)
	err := bgdb.kv.Update(func(tx *badger.Txn) error {
		return tx.Set(k, d)
	})
	bgdb.ts.Touch(bgdb.graph)
	return err
}

// SetEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (bgdb *BadgerGDB) SetEdge(edge aql.Edge) error {
	if edge.Gid == "" {
		eid := fmt.Sprintf("%d", rand.Uint64())
		for ; hasKey(bgdb.kv, edgeKeyPrefix(bgdb.graph, eid)); eid = fmt.Sprintf("%d", rand.Uint64()) {
		}
		edge.Gid = eid
	}
	eid := edge.Gid
	data, _ := proto.Marshal(&edge)

	src := edge.From
	dst := edge.To
	ekey := edgeKey(bgdb.graph, eid, src, dst)
	skey := srcEdgeKey(bgdb.graph, src, dst, eid, edge.Label)
	dkey := dstEdgeKey(bgdb.graph, src, dst, eid, edge.Label)

	err := bgdb.kv.Update(func(tx *badger.Txn) error {
		err := tx.SetWithMeta(ekey, data, cEdgeSingle)
		if err != nil {
			return err
		}
		err = tx.SetWithMeta(skey, []byte{}, cEdgeSingle)
		if err != nil {
			return err
		}
		err = tx.SetWithMeta(dkey, []byte{}, cEdgeSingle)
		if err != nil {
			return err
		}
		return nil
	})
	bgdb.ts.Touch(bgdb.graph)
	return err
}

// SetBundle adds a bundle to the graph
func (bgdb *BadgerGDB) SetBundle(bundle aql.Bundle) error {
	if bundle.Gid == "" {
		eid := fmt.Sprintf("%d", rand.Uint64())
		for ; hasKey(bgdb.kv, edgeKeyPrefix(bgdb.graph, eid)); eid = fmt.Sprintf("%d", rand.Uint64()) {
		}
		bundle.Gid = eid
	}
	eid := bundle.Gid
	data, _ := proto.Marshal(&bundle)

	src := bundle.From
	dst := ""
	ekey := edgeKey(bgdb.graph, eid, src, dst)
	skey := srcEdgeKey(bgdb.graph, src, dst, eid, bundle.Label)

	err := bgdb.kv.Update(func(tx *badger.Txn) error {
		if err := tx.SetWithMeta(ekey, data, cEdgeBundle); err != nil {
			return err
		}
		if err := tx.SetWithMeta(skey, []byte{}, cEdgeBundle); err != nil {
			return err
		}
		return nil
	})
	bgdb.ts.Touch(bgdb.graph)
	return err
}

// DelEdge deletes edge with id `key`
func (bgdb *BadgerGDB) DelEdge(eid string) error {
	ekeyPrefix := edgeKeyPrefix(bgdb.graph, eid)
	var ekey []byte
	bgdb.kv.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), ekeyPrefix); it.Next() {
			ekey = it.Item().Key()
		}
		return nil
	})

	if ekey == nil {
		return fmt.Errorf("Edge Not Found")
	}

	_, _, sid, did := edgeKeyParse(ekey)

	skey := srcEdgeKeyPrefix(bgdb.graph, sid, did, eid)
	dkey := dstEdgeKeyPrefix(bgdb.graph, sid, did, eid)

	err := bgdb.kv.Update(func(tx *badger.Txn) error {
		if err := tx.Delete(ekey); err != nil {
			return err
		}
		if err := tx.Delete(skey); err != nil {
			return err
		}
		if err := tx.Delete(dkey); err != nil {
			return err
		}
		return nil
	})
	bgdb.ts.Touch(bgdb.graph)
	return err
}

// DelBundle removes a bundle of edges given an id
func (bgdb *BadgerGDB) DelBundle(eid string) error {
	ekeyPrefix := edgeKeyPrefix(bgdb.graph, eid)
	var ekey []byte
	bgdb.kv.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), ekeyPrefix); it.Next() {
			ekey = it.Item().Key()
		}
		return nil
	})
	if ekey == nil {
		return fmt.Errorf("Edge Not Found")
	}

	_, _, sid, _ := edgeKeyParse(ekey)

	skey := srcEdgeKeyPrefix(bgdb.graph, sid, "", eid)

	err := bgdb.kv.Update(func(tx *badger.Txn) error {
		if err := tx.Delete(ekey); err != nil {
			return err
		}
		if err := tx.Delete(skey); err != nil {
			return err
		}
		return nil
	})
	bgdb.ts.Touch(bgdb.graph)
	return err
}

// DelVertex deletes vertex with id `key`
func (bgdb *BadgerGDB) DelVertex(id string) error {
	vid := vertexKey(bgdb.graph, id)
	sKeyPrefix := srcEdgePrefix(bgdb.graph, id)
	dKeyPrefix := dstEdgePrefix(bgdb.graph, id)

	delKeys := make([][]byte, 0, 1000)

	bgdb.kv.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(sKeyPrefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), sKeyPrefix); it.Next() {
			skey := it.Item().Key()
			// get edge ID from key
			_, sid, did, eid, _ := srcEdgeKeyParse(skey)
			ekey := edgeKey(bgdb.graph, eid, sid, did)
			delKeys = append(delKeys, skey, ekey)
		}
		for it.Seek(dKeyPrefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), dKeyPrefix); it.Next() {
			dkey := it.Item().Key()
			// get edge ID from key
			_, sid, did, eid, _ := srcEdgeKeyParse(dkey)
			ekey := edgeKey(bgdb.graph, eid, sid, did)
			delKeys = append(delKeys, ekey)
		}
		return nil
	})

	err := bgdb.kv.Update(func(tx *badger.Txn) error {
		if err := tx.Delete(vid); err != nil {
			return err
		}
		for _, k := range delKeys {
			if err := tx.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
	bgdb.ts.Touch(bgdb.graph)
	return err
}

// GetEdgeList produces a channel of all edges in the graph
func (bgdb *BadgerGDB) GetEdgeList(ctx context.Context, loadProp bool) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		bgdb.kv.View(func(tx *badger.Txn) error {
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			ePrefix := edgeListPrefix(bgdb.graph)
			for it.Seek(ePrefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), ePrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Item().Key()
				_, eid, sid, did := edgeKeyParse(keyValue)
				if it.Item().UserMeta() == cEdgeSingle {
					if loadProp {
						edgeData, _ := it.Item().Value()
						e := aql.Edge{}
						proto.Unmarshal(edgeData, &e)
						o <- e
					} else {
						e := aql.Edge{Gid: string(eid), From: sid, To: did}
						o <- e
					}
				} else {
					bundle := aql.Bundle{}
					edgeData, _ := it.Item().Value()
					proto.Unmarshal(edgeData, &bundle)
					for k, v := range bundle.Bundle {
						e := aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Data: v}
						o <- e
					}
				}
			}
			return nil
		})
	}()
	return o
}

// GetInEdgeList given vertex `key` find all incoming edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (bgdb *BadgerGDB) GetInEdgeList(ctx context.Context, id string, loadProp bool, edgeLabels []string) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		dKeyPrefix := dstEdgePrefix(bgdb.graph, id)
		bgdb.kv.View(func(tx *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := tx.NewIterator(opts)
			defer it.Close()
			for it.Seek(dKeyPrefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), dKeyPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Item().Key()
				_, src, dst, eid, label := dstEdgeKeyParse(keyValue)
				e := aql.Edge{}
				if len(edgeLabels) == 0 || contains(edgeLabels, label) {
					if loadProp {
						ekey := edgeKey(bgdb.graph, eid, src, dst)
						dataValue, err := tx.Get(ekey)
						if err == nil {
							d, _ := dataValue.Value()
							proto.Unmarshal(d, &e)
						}
					} else {
						e.Gid = string(eid)
						e.From = string(src)
						e.Label = label
						e.To = dst
					}
					o <- e
				}
			}
			return nil
		})
	}()
	return o
}

// GetOutEdgeList given vertex `key` find all outgoing edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (bgdb *BadgerGDB) GetOutEdgeList(ctx context.Context, id string, loadProp bool, edgeLabels []string) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		//log.Printf("GetOutList")
		sKeyPrefix := srcEdgePrefix(bgdb.graph, id)
		bgdb.kv.View(func(tx *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := tx.NewIterator(opts)
			defer it.Close()
			for it.Seek(sKeyPrefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), sKeyPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Item().Key()
				_, src, dst, eid, label := srcEdgeKeyParse(keyValue)
				if len(edgeLabels) == 0 || contains(edgeLabels, label) {
					if it.Item().UserMeta() == cEdgeSingle {
						e := aql.Edge{}
						if loadProp {
							ekey := edgeKey(bgdb.graph, eid, src, dst)
							dataValue, err := tx.Get(ekey)
							if err == nil {
								d, _ := dataValue.Value()
								proto.Unmarshal(d, &e)
							}
						} else {
							e.Gid = string(eid)
							e.From = string(src)
							e.To = dst
							e.Label = label
						}
						o <- e
					} else if it.Item().UserMeta() == cEdgeBundle {
						bundle := aql.Bundle{}
						ekey := edgeKey(bgdb.graph, eid, src, "")
						dataValue, err := tx.Get(ekey)
						if err == nil {
							d, _ := dataValue.Value()
							proto.Unmarshal(d, &bundle)
							for k, v := range bundle.Bundle {
								e := aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Data: v}
								o <- e
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

// GetOutBundleList given vertex `key` find all outgoing bundles,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
// load is ignored
func (bgdb *BadgerGDB) GetOutBundleList(ctx context.Context, id string, load bool, edgeLabels []string) chan aql.Bundle {
	o := make(chan aql.Bundle, 100)
	go func() {
		defer close(o)
		bgdb.kv.View(func(tx *badger.Txn) error {
			sKeyPrefix := srcEdgePrefix(bgdb.graph, id)
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := tx.NewIterator(opts)
			defer it.Close()
			for it.Seek(sKeyPrefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), sKeyPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Item().Key()
				_, src, _, eid, label := srcEdgeKeyParse(keyValue)
				if len(edgeLabels) == 0 || contains(edgeLabels, label) {
					if it.Item().UserMeta() == cEdgeBundle {
						bundle := aql.Bundle{}
						ekey := edgeKey(bgdb.graph, eid, src, "")
						dataValue, err := tx.Get(ekey)
						if err == nil {
							d, _ := dataValue.Value()
							proto.Unmarshal(d, &bundle)
							o <- bundle
						}
					}
				}
			}
			return nil
		})
	}()
	return o
}

// GetInList given vertex/edge `key` find vertices on incoming edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (bgdb *BadgerGDB) GetInList(ctx context.Context, id string, loadProp bool, edgeLabels []string) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)

		bgdb.kv.View(func(tx *badger.Txn) error {
			dKeyPrefix := dstEdgePrefix(bgdb.graph, id)
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := tx.NewIterator(opts)
			defer it.Close()
			for it.Seek(dKeyPrefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), dKeyPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Item().Key()
				_, src, _, _, label := dstEdgeKeyParse(keyValue)
				if len(edgeLabels) == 0 || contains(edgeLabels, label) {
					vkey := vertexKey(bgdb.graph, src)
					dataValue, err := tx.Get(vkey)
					if err == nil {
						d, _ := dataValue.Value()
						v := aql.Vertex{}
						proto.Unmarshal(d, &v)
						o <- v
					}
				}
			}
			return nil
		})
	}()
	return o
}

// GetOutList given vertex/edge `key` find vertices on outgoing edges,
// if len(edgeLabels) > 0 the edge labels must match a string in the array
func (bgdb *BadgerGDB) GetOutList(ctx context.Context, id string, loadProp bool, edgeLabels []string) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	vertexChan := make(chan []byte, 100)
	go func() {
		defer close(vertexChan)
		bgdb.kv.View(func(tx *badger.Txn) error {
			sKeyPrefix := srcEdgePrefix(bgdb.graph, id)
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := tx.NewIterator(opts)
			defer it.Close()

			for it.Seek(sKeyPrefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), sKeyPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				keyValue := it.Item().Key()
				_, src, dst, eid, label := srcEdgeKeyParse(keyValue)
				if len(edgeLabels) == 0 || contains(edgeLabels, label) {
					vkey := vertexKey(bgdb.graph, dst)
					if it.Item().UserMeta() == cEdgeSingle {
						vertexChan <- vkey
					} else if it.Item().UserMeta() == cEdgeBundle {
						bkey := edgeKey(bgdb.graph, eid, src, "")
						bundleValue, err := tx.Get(bkey)
						if err == nil {
							bundle := aql.Bundle{}
							data, _ := bundleValue.Value()
							proto.Unmarshal(data, &bundle)
							for k := range bundle.Bundle {
								vertexChan <- vertexKey(bgdb.graph, k)
							}
						}
					}
				}
			}
			return nil
		})
	}()

	go func() {
		defer close(o)
		bgdb.kv.View(func(tx *badger.Txn) error {
			for vkey := range vertexChan {
				dataValue, err := tx.Get(vkey)
				if err == nil {
					d, _ := dataValue.Value()
					v := aql.Vertex{}
					proto.Unmarshal(d, &v)
					o <- v
				}
			}
			return nil
		})
	}()
	return o
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (bgdb *BadgerGDB) GetVertex(id string, loadProp bool) *aql.Vertex {
	vkey := vertexKey(bgdb.graph, id)
	v := aql.Vertex{}
	bgdb.kv.View(func(tx *badger.Txn) error {
		dataValue, err := tx.Get(vkey)
		if err != nil {
			return nil
		}
		if loadProp {
			d, _ := dataValue.Value()
			proto.Unmarshal(d, &v)
		} else {
			v.Gid = id
		}
		return nil
	})
	return &v
}

// GetVertexListByID is passed a channel of vertex ids and it produces a channel
// of vertices
func (bgdb *BadgerGDB) GetVertexListByID(ctx context.Context, ids chan string, load bool) chan *aql.Vertex {
	data := make(chan []byte, 100)
	go func() {
		defer close(data)
		bgdb.kv.View(func(tx *badger.Txn) error {
			for id := range ids {
				vkey := vertexKey(bgdb.graph, id)
				dataValue, err := tx.Get(vkey)
				if err == nil {
					d, _ := dataValue.Value()
					data <- d
				} else {
					data <- nil
				}
			}
			return nil
		})
	}()

	out := make(chan *aql.Vertex, 100)
	go func() {
		defer close(out)
		for d := range data {
			if d != nil {
				v := aql.Vertex{}
				proto.Unmarshal(d, &v)
				out <- &v
			} else {
				out <- nil
			}
		}
	}()

	return out
}

// GetEdge loads an edge given an id. It returns nil if not found
func (bgdb *BadgerGDB) GetEdge(id string, loadProp bool) *aql.Edge {
	ekeyPrefix := edgeKeyPrefix(bgdb.graph, id)

	var e *aql.Edge
	bgdb.kv.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), ekeyPrefix); it.Next() {
			_, eid, src, dst := edgeKeyParse(it.Item().Key())
			if loadProp {
				e := &aql.Edge{}
				d, _ := it.Item().Value()
				proto.Unmarshal(d, e)
			} else {
				e := &aql.Edge{}
				e.Gid = eid
				e.From = src
				e.To = dst
			}
		}
		return nil
	})
	return e
}

// GetBundle loads bundle of edges, given an id
// loadProp is ignored
func (bgdb *BadgerGDB) GetBundle(id string, load bool) *aql.Bundle {
	ekeyPrefix := edgeKeyPrefix(bgdb.graph, id)

	var e *aql.Bundle
	bgdb.kv.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(ekeyPrefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), ekeyPrefix); it.Next() {
			e := &aql.Bundle{}
			d, _ := it.Item().Value()
			proto.Unmarshal(d, e)
		}
		return nil
	})
	return e
}

// GetVertexList produces a channel of all edges in the graph
func (bgdb *BadgerGDB) GetVertexList(ctx context.Context, loadProp bool) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)
		bgdb.kv.View(func(tx *badger.Txn) error {
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			vPrefix := vertexListPrefix(bgdb.graph)

			for it.Seek(vPrefix); it.Valid() && bytes.HasPrefix(it.Item().Key(), vPrefix); it.Next() {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				v := aql.Vertex{}
				if loadProp {
					dataValue, _ := it.Item().Value()
					proto.Unmarshal(dataValue, &v)
				} else {
					keyValue := it.Item().Key()
					_, vid := vertexKeyParse(keyValue)
					v.Gid = string(vid)
				}
				o <- v
			}
			return nil
		})
	}()
	return o
}
