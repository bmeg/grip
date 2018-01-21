package mongo

import (
	//"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/protoutil"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"gopkg.in/mgo.v2/bson"
)

var fieldLabel = "label"
var fieldSrc = "from"
var fieldDst = "to"
var fieldBundle = "bundle"

// PackVertex take a AQL vertex and convert it to a mongo doc
func PackVertex(v aql.Vertex) map[string]interface{} {
	p := map[string]interface{}{}
	if v.Data != nil {
		p = protoutil.AsMap(v.Data)
	}
	//fmt.Printf("proto:%s\nmap:%s\n", v.Data, p)
	return map[string]interface{}{
		"_id":   v.Gid,
		"label": v.Label,
		"data":  p,
	}
}

// PackEdge takes a AQL edge and converts it to a mongo doc
func PackEdge(e aql.Edge) map[string]interface{} {
	p := map[string]interface{}{}
	if e.Data != nil {
		p = protoutil.AsMap(e.Data)
	}
	o := map[string]interface{}{
		fieldSrc: e.From,
		fieldDst: e.To,
		"label":   e.Label,
		"data":    p,
	}
	if e.Gid != "" {
		o["_id"] = e.Gid
	}
	return o
}

type pair struct {
	key          string
	valueMap    interface{}
	valueStruct *structpb.Struct
}

// PackBundle takes an AQL edge bundle and converts it into a mongo doc
func PackBundle(e aql.Bundle) map[string]interface{} {
	m := map[string]interface{}{}

	p1 := make(chan pair, 100)
	go func() {
		for k, v := range e.Bundle {
			p1 <- pair{key: k, valueStruct: v}
		}
		close(p1)
	}()

	p2 := make(chan pair, 100)
	pclose := make(chan bool)
	NWORKERS := 8
	for i := 0; i < NWORKERS; i++ {
		go func() {
			for i := range p1 {
				p2 <- pair{key: i.key, valueMap: protoutil.AsMap(i.valueStruct)}
			}
			pclose <- true
		}()
	}
	go func() {
		for i := 0; i < NWORKERS; i++ {
			<-pclose
		}
		close(p2)
	}()

	for i := range p2 {
		m[i.key] = i.valueMap
	}

	o := map[string]interface{}{
		fieldSrc:    e.From,
		fieldBundle: m,
		"label":      e.Label,
	}
	if e.Gid != "" {
		o["_id"] = e.Gid
	}
	return o
}

// UnpackVertex takes a mongo doc and converts it into an aql.Vertex
func UnpackVertex(i map[string]interface{}) aql.Vertex {
	o := aql.Vertex{}
	o.Gid = i["_id"].(string)
	o.Label = i["label"].(string)
	if p, ok := i["data"]; ok {
		o.Data = protoutil.AsStruct(p.(map[string]interface{}))
	}
	return o
}

// UnpackEdge takes a mongo doc and convertes it into an aql.Edge
func UnpackEdge(i map[string]interface{}) aql.Edge {
	o := aql.Edge{}
	id := i["_id"]
	if idb, ok := id.(bson.ObjectId); ok {
		o.Gid = idb.Hex()
	} else {
		o.Gid = id.(string)
	}
	o.Label = i["label"].(string)
	o.From = i[fieldSrc].(string)
	o.To = i[fieldDst].(string)
	o.Data = protoutil.AsStruct(i["data"].(map[string]interface{}))
	return o
}

// UnpackBundle take a mongo doc and converts it into an aql.Bundle
func UnpackBundle(i map[string]interface{}) aql.Bundle {
	o := aql.Bundle{}
	id := i["_id"]
	if idb, ok := id.(bson.ObjectId); ok {
		o.Gid = idb.Hex()
	} else {
		o.Gid = id.(string)
	}
	o.Label = i["label"].(string)
	o.From = i[fieldSrc].(string)
	m := map[string]*structpb.Struct{}

	p1 := make(chan pair, 100)
	go func() {
		for k, v := range i[fieldBundle].(map[string]interface{}) {
			p1 <- pair{k, v, nil}
		}
		close(p1)
	}()
	p2 := make(chan pair, 100)
	pclose := make(chan bool)
	NWORKERS := 8
	for i := 0; i < NWORKERS; i++ {
		go func() {
			for p := range p1 {
				p2 <- pair{p.key, nil, protoutil.AsStruct(p.valueMap.(map[string]interface{}))}
			}
			pclose <- true
		}()
	}
	go func() {
		for i := 0; i < NWORKERS; i++ {
			<-pclose
		}
		close(p2)
	}()

	for p := range p2 {
		m[p.key] = p.valueStruct
	}
	o.Bundle = m
	return o
}
