package mongo

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/protoutil"
	"github.com/bmeg/arachne/timestamp"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"golang.org/x/sync/errgroup"
)

// Config describes the configuration for the mongodb driver.
type Config struct {
	URL                    string
	DBName                 string
	Username               string
	Password               string
	BatchSize              int
	UseAggregationPipeline bool
}

// GraphDB is the base driver that manages multiple graphs in mongo
type GraphDB struct {
	database string
	conf     Config
	session  *mgo.Session
	ts       *timestamp.Timestamp
}

// NewGraphDB creates a new mongo graph database interface
func NewGraphDB(conf Config) (gdbi.GraphDB, error) {
	log.Printf("Starting Mongo Driver")
	database := strings.ToLower(conf.DBName)
	err := aql.ValidateGraphName(database)
	if err != nil {
		return nil, fmt.Errorf("invalid database name: %v", err)
	}

	ts := timestamp.NewTimestamp()
	dialinfo := &mgo.DialInfo{
		Addrs:        []string{conf.URL},
		Database:     conf.DBName,
		Username:     conf.Username,
		Password:     conf.Password,
		AppName:      "arachne",
		ReadTimeout:  0,
		WriteTimeout: 0,
		PoolLimit:    4096,
		PoolTimeout:  0,
		MinPoolSize:  10,
	}
	session, err := mgo.DialWithInfo(dialinfo)
	if err != nil {
		return nil, err
	}
	session.SetSyncTimeout(1 * time.Minute)

	b, _ := session.BuildInfo()
	if !b.VersionAtLeast(3, 6) {
		session.Close()
		return nil, fmt.Errorf("requires mongo 3.6 or later")
	}
	if conf.BatchSize == 0 {
		conf.BatchSize = 1000
	}
	db := &GraphDB{database: database, conf: conf, session: session, ts: &ts}
	for _, i := range db.ListGraphs() {
		db.ts.Touch(i)
	}
	return db, nil
}

// Close the connection
func (ma *GraphDB) Close() error {
	ma.session.Close()
	ma.session = nil
	return nil
}

// VertexCollection returns a *mgo.Collection
func (ma *GraphDB) VertexCollection(session *mgo.Session, graph string) *mgo.Collection {
	return session.DB(ma.database).C(fmt.Sprintf("%s_vertices", graph))
}

// EdgeCollection returns a *mgo.Collection
func (ma *GraphDB) EdgeCollection(session *mgo.Session, graph string) *mgo.Collection {
	return session.DB(ma.database).C(fmt.Sprintf("%s_edges", graph))
}

// AddGraph creates a new graph named `graph`
func (ma *GraphDB) AddGraph(graph string) error {
	err := aql.ValidateGraphName(graph)
	if err != nil {
		return err
	}

	session := ma.session.Copy()
	session.ResetIndexCache()
	defer session.Close()
	defer ma.ts.Touch(graph)

	graphs := session.DB(ma.database).C("graphs")
	err = graphs.Insert(bson.M{"_id": graph})
	if err != nil {
		return fmt.Errorf("failed to insert graph %s: %v", graph, err)
	}

	e := ma.EdgeCollection(session, graph)
	err = e.EnsureIndex(mgo.Index{
		Key:        []string{"from"},
		Unique:     false,
		DropDups:   false,
		Sparse:     false,
		Background: true,
	})
	if err != nil {
		return fmt.Errorf("failed create index for graph %s: %v", graph, err)
	}
	err = e.EnsureIndex(mgo.Index{
		Key:        []string{"to"},
		Unique:     false,
		DropDups:   false,
		Sparse:     false,
		Background: true,
	})
	if err != nil {
		return fmt.Errorf("failed create index for graph %s: %v", graph, err)
	}
	err = e.EnsureIndex(mgo.Index{
		Key:        []string{"label"},
		Unique:     false,
		DropDups:   false,
		Sparse:     false,
		Background: true,
	})
	if err != nil {
		return fmt.Errorf("failed create index for graph %s: %v", graph, err)
	}

	v := ma.VertexCollection(session, graph)
	err = v.EnsureIndex(mgo.Index{
		Key:        []string{"label"},
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
func (ma *GraphDB) DeleteGraph(graph string) error {
	session := ma.session.Copy()
	defer session.Close()
	defer ma.ts.Touch(graph)

	g := session.DB(ma.database).C("graphs")
	v := ma.VertexCollection(session, graph)
	e := ma.EdgeCollection(session, graph)

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

// ListGraphs lists the graphs managed by this driver
func (ma *GraphDB) ListGraphs() []string {
	session := ma.session.Copy()
	session.SetCursorTimeout(0)
	defer session.Close()

	out := make([]string, 0, 100)
	g := session.DB(ma.database).C("graphs")

	iter := g.Find(nil).Iter()
	defer iter.Close()
	result := map[string]interface{}{}
	for iter.Next(&result) {
		out = append(out, result["_id"].(string))
	}
	if err := iter.Close(); err != nil {
		log.Println("ListGraphs error:", err)
	}

	return out
}

// Graph obtains the gdbi.DBI for a particular graph
func (ma *GraphDB) Graph(graph string) (gdbi.GraphInterface, error) {
	found := false
	for _, gname := range ma.ListGraphs() {
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

// GetSchema returns the schema of a specific graph in the database
func (ma *GraphDB) GetSchema(graph string, sampleN uint32) (*aql.GraphSchema, error) {
	var vSchema []*aql.Vertex
	var eSchema []*aql.Edge
	var g errgroup.Group

	g.Go(func() error {
		var err error
		vSchema, err = ma.getVertexSchema(graph, sampleN)
		if err != nil {
			return fmt.Errorf("getting vertex schema: %v", err)
		}
		return nil
	})

	g.Go(func() error {
		var err error
		eSchema, err = ma.getEdgeSchema(graph, sampleN)
		if err != nil {
			return fmt.Errorf("getting edge schema: %v", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	schema := &aql.GraphSchema{Vertices: vSchema, Edges: eSchema}
	// log.Printf("Graph schema: %+v", schema)
	return schema, nil
}

func (ma *GraphDB) getVertexSchema(graph string, n uint32) ([]*aql.Vertex, error) {
	session := ma.session.Copy()
	defer session.Close()
	v := ma.VertexCollection(session, graph)

	var labels []string
	err := v.Find(nil).Distinct("label", &labels)
	if err != nil {
		return nil, err
	}

	schemaChan := make(chan *aql.Vertex)
	var g errgroup.Group

	for _, label := range labels {
		label := label
		if label == "" {
			continue
		}
		g.Go(func() error {
			log.Printf("vertex label: %s: starting schema build", label)

			session := ma.session.Copy()
			session.SetCursorTimeout(0)
			defer session.Close()
			v := ma.VertexCollection(session, graph)

			pipe := []bson.M{
				{
					"$match": bson.M{
						"label": bson.M{"$eq": label},
					},
				},
				{"$sample": bson.M{"size": n}},
			}

			iter := v.Pipe(pipe).AllowDiskUse().Iter()
			defer iter.Close()
			result := make(map[string]interface{})
			schema := make(map[string]interface{})
			for iter.Next(&result) {
				ds := GetDataFieldTypes(result["data"].(map[string]interface{}))
				MergeMaps(schema, ds)
			}
			if err := iter.Close(); err != nil {
				err = fmt.Errorf("iter error building schema for label %s: %v", label, err)
				log.Printf(err.Error())
				return err
			}

			vSchema := &aql.Vertex{Label: label, Data: protoutil.AsStruct(schema)}
			schemaChan <- vSchema
			log.Printf("vertex label: %s: finished schema build", label)

			return nil
		})
	}

	output := []*aql.Vertex{}
	done := make(chan interface{})
	go func() {
		for s := range schemaChan {
			// log.Printf("Vertex schema: %+v", s)
			output = append(output, s)
		}
		close(done)
	}()

	err = g.Wait()
	close(schemaChan)
	<-done
	return output, err
}

func (ma *GraphDB) getEdgeSchema(graph string, n uint32) ([]*aql.Edge, error) {
	session := ma.session.Copy()
	defer session.Close()
	e := ma.EdgeCollection(session, graph)

	var labels []string
	err := e.Find(nil).Distinct("label", &labels)
	if err != nil {
		return nil, err
	}

	schemaChan := make(chan *aql.Edge)
	var g errgroup.Group

	for _, label := range labels {
		label := label
		if label == "" {
			continue
		}
		g.Go(func() error {
			log.Printf("edge label: %s: starting schema build", label)

			session := ma.session.Copy()
			session.SetCursorTimeout(0)
			defer session.Close()
			e := ma.EdgeCollection(session, graph)

			pipe := []bson.M{
				{
					"$match": bson.M{
						"label": bson.M{"$eq": label},
					},
				},
				{"$sample": bson.M{"size": n}},
			}

			iter := e.Pipe(pipe).AllowDiskUse().Iter()
			defer iter.Close()
			result := make(map[string]interface{})
			schema := make(map[string]interface{})
			fromToPairs := make(fromto)

			for iter.Next(&result) {
				fromToPairs.Add(fromtokey{result["from"].(string), result["to"].(string)})
				ds := GetDataFieldTypes(result["data"].(map[string]interface{}))
				MergeMaps(schema, ds)
			}
			if err := iter.Close(); err != nil {
				err = fmt.Errorf("iter error building schema for label %s: %v", label, err)
				log.Printf(err.Error())
				return err
			}

			fromToPairs = ma.resolveLabels(graph, fromToPairs)
			from := fromToPairs.GetFrom()
			to := fromToPairs.GetTo()

			for j := 0; j < len(from); j++ {
				eSchema := &aql.Edge{Label: label, From: from[j], To: to[j], Data: protoutil.AsStruct(schema)}
				schemaChan <- eSchema
			}
			log.Printf("edge label: %s: finished schema build", label)

			return nil
		})
	}

	output := []*aql.Edge{}
	done := make(chan interface{})
	go func() {
		for s := range schemaChan {
			// log.Printf("Edge schema: %+v", s)
			output = append(output, s)
		}
		close(done)
	}()

	err = g.Wait()
	close(schemaChan)
	<-done
	return output, err
}

type fromtokey struct {
	from, to string
}

type fromto map[fromtokey]interface{}

func (ft fromto) Add(k fromtokey) bool {
	if k.from != "" && k.to != "" {
		// only keep if both from and to labels are valid
		ft[k] = nil
		return true
	}
	return false
}

func (ft fromto) GetFrom() []string {
	out := []string{}
	for k := range ft {
		out = append(out, k.from)
	}
	return out
}

func (ft fromto) GetTo() []string {
	out := []string{}
	for k := range ft {
		out = append(out, k.to)
	}
	return out
}

func (ma *GraphDB) resolveLabels(graph string, ft fromto) fromto {
	out := make([]fromtokey, len(ft))
	var g errgroup.Group

	fromIDs := ft.GetFrom()
	toIDs := ft.GetTo()

	for i := 0; i < len(fromIDs); i++ {
		i := i
		toID := toIDs[i]
		fromID := fromIDs[i]

		g.Go(func() error {
			session := ma.session.Copy()
			defer session.Close()
			col := ma.VertexCollection(session, graph)

			from := ""
			to := ""
			result := map[string]string{}
			err := col.FindId(fromID).Select(bson.M{"_id": -1, "label": 1}).One(&result)
			if err == nil {
				from = result["label"]
			}
			result = map[string]string{}
			err = col.FindId(toID).Select(bson.M{"_id": -1, "label": 1}).One(&result)
			if err == nil {
				to = result["label"]
			}
			out[i] = fromtokey{from, to}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil
	}

	outMap := make(fromto)
	for _, k := range out {
		outMap.Add(k)
	}

	return outMap
}

// MergeMaps deeply merges two maps
func MergeMaps(x1, x2 interface{}) interface{} {
	switch x1 := x1.(type) {
	case map[string]interface{}:
		x2, ok := x2.(map[string]interface{})
		if !ok {
			return x1
		}
		for k, v2 := range x2 {
			if v1, ok := x1[k]; ok {
				x1[k] = MergeMaps(v1, v2)
			} else {
				x1[k] = v2
			}
		}
	case nil:
		x2, ok := x2.(map[string]interface{})
		if ok {
			return x2
		}
	}
	return x1
}

// GetDataFieldTypes iterates over the data map and determines the type of each field
func GetDataFieldTypes(data map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{})
	for key, val := range data {
		if vMap, ok := val.(map[string]interface{}); ok {
			out[key] = GetDataFieldTypes(vMap)
			continue
		}
		if vSlice, ok := val.([]interface{}); ok {
			var vType interface{}
			vType = []interface{}{aql.FieldType_UNKNOWN.String()}
			if len(vSlice) > 0 {
				vSliceVal := vSlice[0]
				if vSliceValMap, ok := vSliceVal.(map[string]interface{}); ok {
					vType = []map[string]interface{}{GetDataFieldTypes(vSliceValMap)}
				} else {
					vType = []interface{}{GetFieldType(vSliceVal)}
				}
			}
			out[key] = vType
			continue
		}
		out[key] = GetFieldType(val)
	}
	return out
}

// GetFieldType returns the aql.FieldType for a value
func GetFieldType(field interface{}) string {
	switch field.(type) {
	case string:
		return aql.FieldType_STRING.String()
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return aql.FieldType_NUMERIC.String()
	case float32, float64:
		return aql.FieldType_NUMERIC.String()
	case bool:
		return aql.FieldType_BOOL.String()
	default:
		return aql.FieldType_UNKNOWN.String()
	}
}
