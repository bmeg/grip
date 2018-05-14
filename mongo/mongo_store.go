package mongo

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/timestamp"
	"github.com/bmeg/arachne/util"
	"github.com/vsco/mgopool"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Config describes the configuration for the mongodb driver.
type Config struct {
	URL       string
	DBName    string
	BatchSize int
}

// Mongo is the base driver that manages multiple graphs in mongo
type Mongo struct {
	database       string
	conf           Config
	initialSession *mgo.Session
	pool           mgopool.Pool
	ts             *timestamp.Timestamp
}

// NewMongo creates a new mongo graph database interface
func NewMongo(conf Config) (gdbi.GraphDB, error) {
	log.Printf("Starting Mongo Driver")
	database := strings.ToLower(conf.DBName)
	err := aql.ValidateGraphName(database)
	if err != nil {
		return nil, fmt.Errorf("invalid database name: %v", err)
	}

	ts := timestamp.NewTimestamp()
	session, err := mgo.Dial(conf.URL)
	if err != nil {
		return nil, err
	}
	b, _ := session.BuildInfo()
	if !b.VersionAtLeast(3, 2) {
		session.Close()
		return nil, fmt.Errorf("requires mongo 3.2 or later")
	}
	pool := mgopool.NewLeaky(session, 3)
	if conf.BatchSize == 0 {
		conf.BatchSize = 1000
	}
	db := &Mongo{database: database, conf: conf, pool: pool, initialSession: session, ts: &ts}
	for _, i := range db.GetGraphs() {
		db.ts.Touch(i)
	}
	return db, nil
}

// Close the connection
func (ma *Mongo) Close() error {
	ma.pool.Close()
	ma.initialSession.Close()
	ma.initialSession = nil
	return nil
}

func (ma *Mongo) getVertexCollection(session *mgo.Session, graph string) *mgo.Collection {
	return session.DB(ma.database).C(fmt.Sprintf("%s_vertices", graph))
}

func (ma *Mongo) getEdgeCollection(session *mgo.Session, graph string) *mgo.Collection {
	return session.DB(ma.database).C(fmt.Sprintf("%s_edges", graph))
}

// Graph is the tnterface to a single graph
type Graph struct {
	ar        *Mongo
	ts        *timestamp.Timestamp //BUG: This timestamp implementation doesn't work againt multiple mongo clients
	graph     string
	batchSize int
}

// AddGraph creates a new graph named `graph`
func (ma *Mongo) AddGraph(graph string) error {
	err := aql.ValidateGraphName(graph)
	if err != nil {
		return err
	}

	session := ma.pool.Get()
	session.ResetIndexCache()
	defer ma.pool.Put(session)
	defer ma.ts.Touch(graph)

	graphs := session.DB(ma.database).C("graphs")
	err = graphs.Insert(bson.M{"_id": graph})
	if err != nil {
		return fmt.Errorf("failed to insert graph %s: %v", graph, err)
	}

	e := ma.getEdgeCollection(session, graph)
	err = e.EnsureIndex(mgo.Index{
		Key:        []string{"$hashed:from"},
		Unique:     false,
		DropDups:   false,
		Sparse:     false,
		Background: true,
	})
	if err != nil {
		return fmt.Errorf("failed create index for graph %s: %v", graph, err)
	}
	err = e.EnsureIndex(mgo.Index{
		Key:        []string{"$hashed:to"},
		Unique:     false,
		DropDups:   false,
		Sparse:     false,
		Background: true,
	})
	if err != nil {
		return fmt.Errorf("failed create index for graph %s: %v", graph, err)
	}
	err = e.EnsureIndex(mgo.Index{
		Key:        []string{"$hashed:label"},
		Unique:     false,
		DropDups:   false,
		Sparse:     false,
		Background: true,
	})
	if err != nil {
		return fmt.Errorf("failed create index for graph %s: %v", graph, err)
	}

	v := ma.getVertexCollection(session, graph)
	err = v.EnsureIndex(mgo.Index{
		Key:        []string{"$hashed:label"},
		Unique:     false,
		DropDups:   false,
		Sparse:     false,
		Background: true,
	})
	if err != nil {
		return fmt.Errorf("failed create index for graph %s: %v", graph, err)
	}

	return nil
}

// DeleteGraph deletes `graph`
func (ma *Mongo) DeleteGraph(graph string) error {
	session := ma.pool.Get()
	defer ma.pool.Put(session)
	defer ma.ts.Touch(graph)

	g := session.DB(ma.database).C("graphs")
	v := ma.getVertexCollection(session, graph)
	e := ma.getEdgeCollection(session, graph)

	verr := v.DropCollection()
	if verr != nil {
		log.Printf("Drop vertex collection failed: %v", verr)
	}
	eerr := e.DropCollection()
	if eerr != nil {
		log.Printf("Drop edge collection failed: %v", eerr)
	}
	gerr := g.RemoveId(graph)
	if gerr != nil {
		log.Printf("Remove graph id failed: %v", gerr)
	}

	if verr != nil || eerr != nil || gerr != nil {
		return fmt.Errorf("failed to delete graph: %s; %s; %s", verr, eerr, gerr)
	}

	return nil
}

// GetGraphs lists the graphs managed by this driver
func (ma *Mongo) GetGraphs() []string {
	session := ma.pool.Get()
	defer ma.pool.Put(session)

	out := make([]string, 0, 100)
	g := session.DB(ma.database).C("graphs")

	iter := g.Find(nil).Iter()
	defer iter.Close()
	if err := iter.Err(); err != nil {
		log.Printf("Error: %s", err)
	}
	result := map[string]interface{}{}
	for iter.Next(&result) {
		out = append(out, result["_id"].(string))
	}
	if err := iter.Err(); err != nil {
		log.Println("GetGraphs error:", err)
	}

	return out
}

// Graph obtains the gdbi.DBI for a particular graph
func (ma *Mongo) Graph(graph string) (gdbi.GraphInterface, error) {
	found := false
	for _, gname := range ma.GetGraphs() {
		if graph == gname {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("graph '%s' was not found", graph)
	}
	return &Graph{
		ar:        ma,
		ts:        ma.ts,
		graph:     graph,
		batchSize: ma.conf.BatchSize,
	}, nil
}

// Compiler returns a query compiler that uses the graph
func (mg *Graph) Compiler() gdbi.Compiler {
	return NewCompiler(mg)
}

// GetTimestamp gets the timestamp of last update
func (mg *Graph) GetTimestamp() string {
	return mg.ts.Get(mg.graph)
}

// GetVertex loads a vertex given an id. It returns a nil if not found
func (mg *Graph) GetVertex(key string, load bool) *aql.Vertex {
	session := mg.ar.pool.Get()
	defer mg.ar.pool.Put(session)

	d := map[string]interface{}{}
	q := mg.ar.getVertexCollection(session, mg.graph).FindId(key)
	if !load {
		q = q.Select(map[string]interface{}{"_id": 1, "label": 1})
	}
	err := q.One(d)
	if err != nil {
		return nil
	}

	v := UnpackVertex(d)
	return v
}

// GetEdge loads an edge given an id. It returns nil if not found
func (mg *Graph) GetEdge(id string, load bool) *aql.Edge {
	session := mg.ar.pool.Get()
	defer mg.ar.pool.Put(session)

	d := map[string]interface{}{}
	q := mg.ar.getEdgeCollection(session, mg.graph).FindId(id)
	if !load {
		q = q.Select(map[string]interface{}{"_id": 1, "label": 1, "from": 1, "to": 1})
	}
	err := q.One(d)
	if err != nil {
		return nil
	}

	v := UnpackEdge(d)
	return v
}

// MaxRetries is the number of times driver will reconnect on connection failure
// TODO, move to per instance config, rather then global
var MaxRetries = 3

func isNetError(e error) bool {
	if e == io.EOF {
		return true
	}
	if b, ok := e.(*mgo.BulkError); ok {
		for _, c := range b.Cases() {
			if c.Err == io.EOF {
				return true
			}
			if strings.Contains(c.Err.Error(), "connection") {
				return true
			}
		}
	}
	return false
}

// AddVertex adds an edge to the graph, if it already exists
// in the graph, it is replaced
func (mg *Graph) AddVertex(vertexArray []*aql.Vertex) error {
	for _, vertex := range vertexArray {
		err := vertex.Validate()
		if err != nil {
			return fmt.Errorf("vertex validation failed: %v", err)
		}
	}

	session := mg.ar.pool.Get()
	defer mg.ar.pool.Put(session)
	vCol := mg.ar.getVertexCollection(session, mg.graph)
	var err error
	for i := 0; i < MaxRetries; i++ {
		bulk := vCol.Bulk()
		for _, vertex := range vertexArray {
			bulk.Upsert(bson.M{"_id": vertex.Gid}, PackVertex(vertex))
		}
		_, err = bulk.Run()
		if err == nil || !isNetError(err) {
			mg.ts.Touch(mg.graph)
			return err
		}
		log.Printf("Refreshing mongo connection")
		session.Refresh()
	}
	return err
}

// AddEdge adds an edge to the graph, if the id is not "" and in already exists
// in the graph, it is replaced
func (mg *Graph) AddEdge(edgeArray []*aql.Edge) error {
	for _, edge := range edgeArray {
		if edge.Gid == "" {
			edge.Gid = util.UUID()
		}
		err := edge.Validate()
		if err != nil {
			return fmt.Errorf("edge validation failed: %v", err)
		}
	}

	session := mg.ar.pool.Get()
	defer mg.ar.pool.Put(session)
	eCol := mg.ar.getEdgeCollection(session, mg.graph)
	var err error
	for i := 0; i < MaxRetries; i++ {
		bulk := eCol.Bulk()
		for _, edge := range edgeArray {
			bulk.Upsert(bson.M{"_id": edge.Gid}, PackEdge(edge))
		}
		_, err = bulk.Run()
		if err == nil || !isNetError(err) {
			mg.ts.Touch(mg.graph)
			return err
		}
		log.Printf("Refreshing mongo connection")
		session.Refresh()
	}
	return err
}

// deleteConnectedEdges deletes edges where `from` or `to` equal `key`
func (mg *Graph) deleteConnectedEdges(key string) error {
	session := mg.ar.pool.Get()
	defer mg.ar.pool.Put(session)
	eCol := mg.ar.getEdgeCollection(session, mg.graph)
	_, err := eCol.RemoveAll(bson.M{"$or": []bson.M{{"from": key}, {"to": key}}})
	if err != nil {
		return fmt.Errorf("failed to delete edge(s): %s", err)
	}
	mg.ts.Touch(mg.graph)
	return nil
}

// DelVertex deletes vertex with id `key`
func (mg *Graph) DelVertex(key string) error {
	session := mg.ar.pool.Get()
	defer mg.ar.pool.Put(session)

	vCol := mg.ar.getVertexCollection(session, mg.graph)
	err := vCol.RemoveId(key)
	if err != nil {
		return fmt.Errorf("failed to delete vertex %s: %s", key, err)
	}
	mg.ts.Touch(mg.graph)
	err = mg.deleteConnectedEdges(key)
	if err != nil {
		return err
	}
	return nil
}

// DelEdge deletes edge with id `key`
func (mg *Graph) DelEdge(key string) error {
	session := mg.ar.pool.Get()
	defer mg.ar.pool.Put(session)

	eCol := mg.ar.getEdgeCollection(session, mg.graph)
	err := eCol.RemoveId(key)
	if err != nil {
		return fmt.Errorf("failed to delete edge %s: %s", key, err)
	}
	mg.ts.Touch(mg.graph)
	return nil
}

// GetVertexList produces a channel of all edges in the graph
func (mg *Graph) GetVertexList(ctx context.Context, load bool) <-chan *aql.Vertex {
	o := make(chan *aql.Vertex, 100)

	go func() {
		defer close(o)
		session := mg.ar.pool.Get()
		defer mg.ar.pool.Put(session)
		vCol := mg.ar.getVertexCollection(session, mg.graph)
		query := vCol.Find(nil)
		if !load {
			query = query.Select(bson.M{"_id": 1, "label": 1})
		}
		iter := query.Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				mg.ar.pool.Put(session)
				return
			default:
			}
			v := UnpackVertex(result)
			o <- v
		}
		if err := iter.Err(); err != nil {
			log.Println("GetVertexList error:", err)
		}
	}()

	return o
}

// GetEdgeList produces a channel of all edges in the graph
func (mg *Graph) GetEdgeList(ctx context.Context, loadProp bool) <-chan *aql.Edge {
	o := make(chan *aql.Edge, 100)
	go func() {
		defer close(o)
		session := mg.ar.pool.Get()
		defer mg.ar.pool.Put(session)
		eCol := mg.ar.getEdgeCollection(session, mg.graph)
		query := eCol.Find(nil)
		if !loadProp {
			query = query.Select(bson.M{"_id": 1, "to": 1, "from": 1, "label": 1})
		}
		iter := query.Iter()
		defer iter.Close()
		result := map[string]interface{}{}
		for iter.Next(&result) {
			select {
			case <-ctx.Done():
				mg.ar.pool.Put(session)
				return
			default:
			}
			if _, ok := result["to"]; ok {
				e := UnpackEdge(result)
				o <- e
			}
		}
		if err := iter.Err(); err != nil {
			log.Println("GetEdgeList error:", err)
		}
	}()
	return o
}

// GetVertexChannel is passed a channel of vertex ids and it produces a channel
// of vertices
func (mg *Graph) GetVertexChannel(ids chan gdbi.ElementLookup, load bool) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, mg.batchSize)
		for id := range ids {
			o = append(o, id)
			if len(o) >= mg.batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, mg.batchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		session := mg.ar.pool.Get()
		defer mg.ar.pool.Put(session)
		vCol := mg.ar.getVertexCollection(session, mg.graph)
		for batch := range batches {
			idBatch := make([]string, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
			}
			query := bson.M{"_id": bson.M{"$in": idBatch}}
			q := vCol.Find(query)
			if !load {
				q = q.Select(map[string]interface{}{"_id": 1, "label": 1})
			}
			iter := q.Iter()
			defer iter.Close()
			chunk := map[string]*aql.Vertex{}
			result := map[string]interface{}{}
			for iter.Next(&result) {
				v := UnpackVertex(result)
				chunk[v.Gid] = v
			}
			if err := iter.Err(); err != nil {
				log.Println("GetVertexChannel error:", err)
			}
			for _, id := range batch {
				if x, ok := chunk[id.ID]; ok {
					id.Vertex = x
					o <- id
				}
			}
		}
	}()
	return o
}

//GetOutChannel process requests of vertex ids and find the connected vertices on outgoing edges
func (mg *Graph) GetOutChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, mg.batchSize)
		for req := range reqChan {
			o = append(o, req)
			if len(o) >= mg.batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, mg.batchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		session := mg.ar.pool.Get()
		defer mg.ar.pool.Put(session)
		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			query := []bson.M{{"$match": bson.M{"from": bson.M{"$in": idBatch}}}}
			if len(edgeLabels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"label": bson.M{"$in": edgeLabels}}})
			}
			vertCol := fmt.Sprintf("%s_vertices", mg.graph)
			query = append(query, bson.M{"$lookup": bson.M{"from": vertCol, "localField": "to", "foreignField": "_id", "as": "dst"}})
			query = append(query, bson.M{"$unwind": "$dst"})
			if load {
				query = append(query, bson.M{"$project": bson.M{"from": true, "dst._id": true, "dst.label": true, "dst.data": true}})
			} else {
				query = append(query, bson.M{"$project": bson.M{"from": true, "dst._id": true, "dst.label": true}})
			}

			eCol := mg.ar.getEdgeCollection(session, mg.graph)
			iter := eCol.Pipe(query).Iter()
			defer iter.Close()
			result := map[string]interface{}{}
			for iter.Next(&result) {
				if dst, ok := result["dst"].(map[string]interface{}); ok {
					v := UnpackVertex(dst)
					r := batchMap[result["from"].(string)]
					for _, ri := range r {
						ri.Vertex = v
						o <- ri
					}
				} else {
					log.Printf("Out error: %s", result["dst"])
				}
			}
			if err := iter.Err(); err != nil {
				log.Println("GetOutChannel error:", err)
			}
		}
	}()
	return o
}

//GetInChannel process requests of vertex ids and find the connected vertices on incoming edges
func (mg *Graph) GetInChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, mg.batchSize)
		for req := range reqChan {
			o = append(o, req)
			if len(o) >= mg.batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, mg.batchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		session := mg.ar.pool.Get()
		defer mg.ar.pool.Put(session)
		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			query := []bson.M{{"$match": bson.M{"to": bson.M{"$in": idBatch}}}}
			if len(edgeLabels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"label": bson.M{"$in": edgeLabels}}})
			}
			vertCol := fmt.Sprintf("%s_vertices", mg.graph)
			query = append(query, bson.M{"$lookup": bson.M{"from": vertCol, "localField": "from", "foreignField": "_id", "as": "src"}})
			query = append(query, bson.M{"$unwind": "$src"})
			if load {
				query = append(query, bson.M{"$project": bson.M{"to": true, "src._id": true, "src.label": true, "src.data": true}})
			} else {
				query = append(query, bson.M{"$project": bson.M{"to": true, "src._id": true, "src.label": true}})
			}

			eCol := mg.ar.getEdgeCollection(session, mg.graph)
			iter := eCol.Pipe(query).Iter()
			defer iter.Close()
			result := map[string]interface{}{}
			for iter.Next(&result) {
				src := result["src"].(map[string]interface{})
				v := UnpackVertex(src)
				r := batchMap[result["to"].(string)]
				for _, ri := range r {
					ri.Vertex = v
					o <- ri
				}
			}
			if err := iter.Err(); err != nil {
				log.Println("GetInChannel error:", err)
			}
		}
	}()
	return o
}

//GetOutEdgeChannel process requests of vertex ids and find the connected outgoing edges
func (mg *Graph) GetOutEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, mg.batchSize)
		for req := range reqChan {
			o = append(o, req)
			if len(o) >= mg.batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, mg.batchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		session := mg.ar.pool.Get()
		defer mg.ar.pool.Put(session)

		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			query := []bson.M{{"$match": bson.M{"from": bson.M{"$in": idBatch}}}}
			if len(edgeLabels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"label": bson.M{"$in": edgeLabels}}})
			}
			eCol := mg.ar.getEdgeCollection(session, mg.graph)
			iter := eCol.Pipe(query).Iter()
			defer iter.Close()
			result := map[string]interface{}{}
			for iter.Next(&result) {
				e := UnpackEdge(result)
				r := batchMap[result["from"].(string)]
				for _, ri := range r {
					ri.Edge = e
					o <- ri
				}
			}
			if err := iter.Err(); err != nil {
				log.Println("GetOutEdgeChannel error:", err)
			}
		}
	}()

	return o
}

//GetInEdgeChannel process requests of vertex ids and find the connected incoming edges
func (mg *Graph) GetInEdgeChannel(reqChan chan gdbi.ElementLookup, load bool, edgeLabels []string) chan gdbi.ElementLookup {
	batches := make(chan []gdbi.ElementLookup, 100)
	go func() {
		defer close(batches)
		o := make([]gdbi.ElementLookup, 0, mg.batchSize)
		for req := range reqChan {
			o = append(o, req)
			if len(o) >= mg.batchSize {
				batches <- o
				o = make([]gdbi.ElementLookup, 0, mg.batchSize)
			}
		}
		batches <- o
	}()

	o := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(o)
		session := mg.ar.pool.Get()
		defer mg.ar.pool.Put(session)

		for batch := range batches {
			idBatch := make([]string, len(batch))
			batchMap := make(map[string][]gdbi.ElementLookup, len(batch))
			for i := range batch {
				idBatch[i] = batch[i].ID
				batchMap[batch[i].ID] = append(batchMap[batch[i].ID], batch[i])
			}
			query := []bson.M{{"$match": bson.M{"to": bson.M{"$in": idBatch}}}}
			if len(edgeLabels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"label": bson.M{"$in": edgeLabels}}})
			}
			eCol := mg.ar.getEdgeCollection(session, mg.graph)
			iter := eCol.Pipe(query).Iter()
			defer iter.Close()
			result := map[string]interface{}{}
			for iter.Next(&result) {
				e := UnpackEdge(result)
				r := batchMap[result["to"].(string)]
				for _, ri := range r {
					ri.Edge = e
					o <- ri
				}
			}
			if err := iter.Err(); err != nil {
				log.Println("GetInEdgeChannel error:", err)
			}
		}
	}()

	return o
}
