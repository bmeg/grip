package mongo

import (
	"context"
	"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
)

func NewMongoArachne(url string, database string) gdbi.ArachneInterface {
	session, err := mgo.Dial(url)
	if err != nil {
		log.Printf("%s", err)
	}
	db := session.DB(database)
	return &MongoArachne{db}
}

type MongoArachne struct {
	db *mgo.Database
}

type MongoGraph struct {
	vertices *mgo.Collection
	edges    *mgo.Collection
}

func (self *MongoArachne) AddGraph(graph string) error {
	graphs := self.db.C(fmt.Sprintf("graphs"))
	graphs.Insert(map[string]string{"_id": graph})
	//db.bmeg_edges.createIndex({"from":"hashed"})
	//db.bmeg_edges.createIndex({"to":"hashed"})

	return nil
}

func (self *MongoArachne) Close() {
	self.db.Logout()
}

func (self *MongoArachne) DeleteGraph(graph string) error {
	g := self.db.C(fmt.Sprintf("graphs"))
	v := self.db.C(fmt.Sprintf("%s_vertices", graph))
	e := self.db.C(fmt.Sprintf("%s_edges", graph))
	v.DropCollection()
	e.DropCollection()
	g.RemoveId(graph)
	return nil
}

func (self *MongoArachne) GetGraphs() []string {
	out := make([]string, 0, 100)
	return out
}

func (self *MongoArachne) Graph(graph string) gdbi.DBI {
	return &MongoGraph{
		self.db.C(fmt.Sprintf("%s_vertices", graph)),
		self.db.C(fmt.Sprintf("%s_edges", graph)),
	}
}

func (self *MongoArachne) Query(graph string) gdbi.QueryInterface {
	return self.Graph(graph).Query()
}

func (self *MongoGraph) Query() gdbi.QueryInterface {
	return gdbi.NewPipeEngine(self)
}

func (self *MongoGraph) GetEdge(id string, loadProp bool) *aql.Edge {
	d := map[string]interface{}{}
	q := self.vertices.FindId(id)
	q.One(d)
	v := UnpackEdge(d)
	return &v
}

func (self *MongoGraph) GetVertex(key string, load bool) *aql.Vertex {
	//log.Printf("GetVertex: %s", key)
	d := map[string]interface{}{}
	q := self.vertices.FindId(key)
	q.One(d)
	v := UnpackVertex(d)
	return &v
}

func (self *MongoGraph) SetVertex(vertex aql.Vertex) error {
	_, err := self.vertices.UpsertId(vertex.Gid, PackVertex(vertex))
	return err
}

func (self *MongoGraph) SetEdge(edge aql.Edge) error {
	if edge.Gid != "" {
		_, err := self.edges.UpsertId(edge.Gid, PackEdge(edge))
		return err
	}
	err := self.edges.Insert(PackEdge(edge))
	return err
}

func (self *MongoGraph) DelVertex(key string) error {
	return self.vertices.RemoveId(key)
}

func (self *MongoGraph) DelEdge(key string) error {
	return self.edges.RemoveId(key)
}

func (self *MongoGraph) GetVertexList(ctx context.Context, load bool) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)
		iter := self.vertices.Find(nil).Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			v := UnpackVertex(result)
			o <- v
		}
	}()
	return o
}

func (self *MongoGraph) GetEdgeList(ctx context.Context, loadProp bool) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		iter := self.edges.Find(nil).Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if _, ok := result[FIELD_DST]; ok {
				e := UnpackEdge(result)
				o <- e
			} else if _, ok := result[FIELD_BUNDLE]; ok {
				bundle := UnpackBundle(result)
				for k, v := range bundle.Bundle {
					e := aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Properties: v}
					o <- e
				}
			}
		}
	}()
	return o
}

func (self *MongoGraph) GetOutList(ctx context.Context, key string, load bool, filter gdbi.EdgeFilter) chan aql.Vertex {
	o := make(chan aql.Vertex, 100)
	vertex_chan := make(chan string, 100)
	go func() {
		defer close(vertex_chan)
		selection := map[string]interface{}{
			FIELD_SRC: key,
		}
		iter := self.edges.Find(selection).Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if _, ok := result[FIELD_DST]; ok {
				if filter != nil {
					e := UnpackEdge(result)
					if filter(e) {
						vertex_chan <- result[FIELD_DST].(string)
					}
				} else {
					vertex_chan <- result[FIELD_DST].(string)
				}
			} else if val, ok := result[FIELD_BUNDLE]; ok {
				if filter != nil {
					bundle := UnpackBundle(result)
					for k, v := range bundle.Bundle {
						e := aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Properties: v}
						if filter(e) {
							vertex_chan <- k
						}
					}
				} else {
					for k, _ := range val.(map[string]interface{}) {
						vertex_chan <- k
					}
				}
			}
		}
	}()

	go func() {
		defer close(o)
		for dst := range vertex_chan {
			q := self.vertices.FindId(dst)
			if !load {
				q = q.Select(map[string]interface{}{"_id": 1, "label": 1})
			}
			d := map[string]interface{}{}
			q.One(d)
			v := UnpackVertex(d)
			o <- v
		}
	}()
	return o
}

func (self *MongoGraph) GetInList(ctx context.Context, key string, load bool, filter gdbi.EdgeFilter) chan aql.Vertex {
	//log.Printf("In %s %s", key, load)
	o := make(chan aql.Vertex, 100)
	go func() {
		defer close(o)
		selection := map[string]interface{}{
			FIELD_DST: key,
		}
		iter := self.edges.Find(selection).Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			send := false
			if filter != nil {
				e := UnpackEdge(result)
				if filter(e) {
					send = true
				}
			} else {
				send = true
			}
			if send {
				q := self.vertices.FindId(result[FIELD_SRC])
				if !load {
					q = q.Select(map[string]interface{}{"_id": 1, "label": 1})
				}
				d := map[string]interface{}{}
				q.One(d)
				v := UnpackVertex(d)
				o <- v
			}
		}
	}()
	return o
}

func (self *MongoGraph) GetOutEdgeList(ctx context.Context, key string, load bool, filter gdbi.EdgeFilter) chan aql.Edge {
	//log.Printf("OutEdge %s %s", key, load)
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		selection := map[string]interface{}{
			FIELD_SRC: key,
		}
		iter := self.edges.Find(selection).Iter()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			if _, ok := result[FIELD_DST]; ok {
				e := UnpackEdge(result)
				if filter != nil {
					if filter(e) {
						o <- e
					}
				} else {
					o <- e
				}
			} else if _, ok := result[FIELD_BUNDLE]; ok {
				bundle := UnpackBundle(result)
				for k, v := range bundle.Bundle {
					e := aql.Edge{Gid: bundle.Gid, Label: bundle.Label, From: bundle.From, To: k, Properties: v}
					if filter != nil {
						if filter(e) {
							o <- e
						}
					} else {
						o <- e
					}
				}
			}
		}
		//log.Printf("OutEdge Done")
	}()
	return o
}

func (self *MongoGraph) GetInEdgeList(ctx context.Context, key string, load bool, filter gdbi.EdgeFilter) chan aql.Edge {
	o := make(chan aql.Edge, 100)
	go func() {
		defer close(o)
		selection := map[string]interface{}{
			FIELD_DST: key,
		}
		iter := self.edges.Find(selection).Iter()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			send := false
			e := UnpackEdge(result)
			if filter != nil {
				if filter(e) {
					send = true
				}
			} else {
				send = true
			}
			if send {
				o <- e
			}
		}
	}()
	return o
}

func (self *MongoGraph) SetBundle(bundle aql.Bundle) error {
	if bundle.Gid != "" {
		_, err := self.edges.UpsertId(bundle.Gid, PackBundle(bundle))
		return err
	}
	err := self.edges.Insert(PackBundle(bundle))
	return err
}

func (self *MongoGraph) GetBundle(id string, loadProp bool) *aql.Bundle {
	d := map[string]interface{}{}
	q := self.edges.FindId(id)
	q.One(d)
	v := UnpackBundle(d)
	return &v
}

func (self *MongoGraph) DelBundle(id string) error {
	return self.edges.RemoveId(id)
}

func (self *MongoGraph) VertexLabelScan(ctx context.Context, label string) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		selection := map[string]interface{}{
			"label": label,
		}
		iter := self.vertices.Find(selection).Select(map[string]interface{}{"_id": 1}).Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			id := result["_id"]
			if idb, ok := id.(bson.ObjectId); ok {
				out <- idb.String()
			} else {
				out <- id.(string)
			}
		}
	}()
	return out
}

func (self *MongoGraph) EdgeLabelScan(ctx context.Context, label string) chan string {
	out := make(chan string, 100)
	go func() {
		defer close(out)
		selection := map[string]interface{}{
			"label": label,
		}
		iter := self.edges.Find(selection).Select(map[string]interface{}{"_id": 1}).Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			id := result["_id"]
			if idb, ok := id.(bson.ObjectId); ok {
				out <- idb.String()
			} else {
				out <- id.(string)
			}
		}
	}()
	return out
}
