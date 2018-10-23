package mongo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/protoutil"
	"github.com/bmeg/grip/timestamp"
	"github.com/bmeg/grip/util"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	log "github.com/sirupsen/logrus"
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
	log.Info("Starting Mongo Driver")
	database := strings.ToLower(conf.DBName)
	err := gripql.ValidateGraphName(database)
	if err != nil {
		return nil, fmt.Errorf("invalid database name: %v", err)
	}

	ts := timestamp.NewTimestamp()
	dialinfo := &mgo.DialInfo{
		Addrs:        []string{conf.URL},
		Database:     conf.DBName,
		Username:     conf.Username,
		Password:     conf.Password,
		AppName:      "grip",
		ReadTimeout:  0,
		WriteTimeout: 0,
		PoolLimit:    4096,
		PoolTimeout:  0,
		MinPoolSize:  100,
	}
	session, err := mgo.DialWithInfo(dialinfo)
	if err != nil {
		return nil, err
	}
	session.SetSyncTimeout(1 * time.Minute)
	session.SetCursorTimeout(0)

	b, _ := session.BuildInfo()
	if !b.VersionAtLeast(3, 6) {
		session.Close()
		return nil, fmt.Errorf("requires mongo 3.6 or later")
	}
	if conf.BatchSize == 0 {
		conf.BatchSize = 1000
	}
	db := &GraphDB{database: database, conf: conf, session: session, ts: &ts}
	for _, g := range db.ListGraphs() {
		g := g
		db.ts.Touch(g)
		go func() {
			err := db.setupIndices(g)
			if err != nil {
				log.WithFields(log.Fields{"error": err, "graph": g}).Error("Setting up indices")
			}
		}()
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
	err := gripql.ValidateGraphName(graph)
	if err != nil {
		return err
	}

	session := ma.session.Copy()
	defer session.Close()
	defer ma.ts.Touch(graph)

	graphs := session.DB(ma.database).C("graphs")
	err = graphs.Insert(bson.M{"_id": graph})
	if err != nil {
		return fmt.Errorf("failed to insert graph %s: %v", graph, err)
	}

	return ma.setupIndices(graph)
}

func (ma *GraphDB) setupIndices(graph string) error {
	session := ma.session.Copy()
	session.ResetIndexCache()
	defer session.Close()

	e := ma.EdgeCollection(session, graph)
	err := e.EnsureIndex(mgo.Index{
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
		log.WithFields(log.Fields{"error": verr, "graph": graph}).Error("DeleteGraph: MongoDB: dropping vertex collection")
	}
	eerr := e.DropCollection()
	if eerr != nil {
		log.WithFields(log.Fields{"error": eerr, "graph": graph}).Error("DeleteGraph: MongoDB: dropping edge collection")
	}
	gerr := g.RemoveId(graph)
	if gerr != nil {
		log.WithFields(log.Fields{"error": gerr, "graph": graph}).Error("DeleteGraph: MongoDB: removing graph id")
	}

	if verr != nil || eerr != nil || gerr != nil {
		return fmt.Errorf("failed to delete graph: %s; %s; %s", verr, eerr, gerr)
	}

	return nil
}

// ListGraphs lists the graphs managed by this driver
func (ma *GraphDB) ListGraphs() []string {
	session := ma.session.Copy()
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
		log.WithFields(log.Fields{"error": err}).Error("ListGraphs: MongoDB: iterating over graphs collection")
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
func (ma *GraphDB) GetSchema(ctx context.Context, graph string, sampleN uint32, random bool) (*gripql.GraphSchema, error) {
	var vSchema []*gripql.Vertex
	var eSchema []*gripql.Edge
	var g errgroup.Group

	g.Go(func() error {
		var err error
		vSchema, err = ma.getVertexSchema(ctx, graph, sampleN, random)
		if err != nil {
			return fmt.Errorf("getting vertex schema: %v", err)
		}
		return nil
	})

	g.Go(func() error {
		var err error
		eSchema, err = ma.getEdgeSchema(ctx, graph, sampleN, random)
		if err != nil {
			return fmt.Errorf("getting edge schema: %v", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	schema := &gripql.GraphSchema{Vertices: vSchema, Edges: eSchema}
	log.WithFields(log.Fields{"graph": graph, "schema": schema}).Debug("Finished GetSchema call")
	return schema, nil
}

func (ma *GraphDB) getVertexSchema(ctx context.Context, graph string, n uint32, random bool) ([]*gripql.Vertex, error) {
	session := ma.session.Copy()
	defer session.Close()
	v := ma.VertexCollection(session, graph)

	var labels []string
	err := v.Find(nil).Distinct("label", &labels)
	if err != nil {
		return nil, err
	}

	schemaChan := make(chan *gripql.Vertex)
	var g errgroup.Group

	for _, label := range labels {
		label := label
		if label == "" {
			continue
		}
		g.Go(func() error {
			log.WithFields(log.Fields{"graph": graph, "label": label}).Debug("getVertexSchema: Started schema build")

			session := ma.session.Copy()
			err := session.Ping()
			if err != nil {
				log.WithFields(log.Fields{"graph": graph, "label": label, "error": err}).Warning("getVertexSchema: Session ping error")
				session.Refresh()
			}
			defer session.Close()
			v := ma.VertexCollection(session, graph)

			pipe := []bson.M{
				{
					"$match": bson.M{
						"label": bson.M{"$eq": label},
					},
				},
			}

			if random {
				pipe = append(pipe, bson.M{"$sample": bson.M{"size": n}})
			} else {
				pipe = append(pipe, bson.M{"$limit": n})
			}

			iter := v.Pipe(pipe).AllowDiskUse().Iter()
			defer iter.Close()
			result := make(map[string]interface{})
			schema := make(map[string]interface{})
			for iter.Next(&result) {
				select {
				case <-ctx.Done():
					return ctx.Err()

				default:
					if result["data"] != nil {
						ds := gripql.GetDataFieldTypes(result["data"].(map[string]interface{}))
						util.MergeMaps(schema, ds)
					}
				}
			}
			if err := iter.Close(); err != nil {
				log.WithFields(log.Fields{"graph": graph, "label": label, "error": err}).Error("getVertexSchema: MongoDB: iter error")
				return err
			}

			vSchema := &gripql.Vertex{Label: label, Data: protoutil.AsStruct(schema)}
			schemaChan <- vSchema
			log.WithFields(log.Fields{"graph": graph, "label": label}).Debug("getVertexSchema: Finished schema build")

			return nil
		})
	}

	output := []*gripql.Vertex{}
	done := make(chan interface{})
	go func() {
		for s := range schemaChan {
			output = append(output, s)
		}
		close(done)
	}()

	err = g.Wait()
	close(schemaChan)
	<-done
	return output, err
}

func (ma *GraphDB) getEdgeSchema(ctx context.Context, graph string, n uint32, random bool) ([]*gripql.Edge, error) {
	session := ma.session.Copy()
	defer session.Close()
	e := ma.EdgeCollection(session, graph)

	var labels []string
	err := e.Find(nil).Distinct("label", &labels)
	if err != nil {
		return nil, err
	}

	schemaChan := make(chan *gripql.Edge)
	var g errgroup.Group

	for _, label := range labels {
		label := label
		if label == "" {
			continue
		}
		g.Go(func() error {
			log.WithFields(log.Fields{"graph": graph, "label": label}).Debug("getEdgeSchema: Started schema build")

			session := ma.session.Copy()
			err := session.Ping()
			if err != nil {
				log.WithFields(log.Fields{"graph": graph, "label": label, "error": err}).Warning("getEdgeSchema: Session ping error")
				session.Refresh()
			}
			defer session.Close()
			e := ma.EdgeCollection(session, graph)

			pipe := []bson.M{
				{
					"$match": bson.M{
						"label": bson.M{"$eq": label},
					},
				},
			}

			if random {
				pipe = append(pipe, bson.M{"$sample": bson.M{"size": n}})
			} else {
				pipe = append(pipe, bson.M{"$limit": n})
			}

			iter := e.Pipe(pipe).AllowDiskUse().Iter()
			defer iter.Close()
			result := make(map[string]interface{})
			schema := make(map[string]interface{})
			fromToPairs := make(fromto)

			for iter.Next(&result) {
				select {
				case <-ctx.Done():
					return ctx.Err()

				default:
					fromToPairs.Add(fromtokey{result["from"].(string), result["to"].(string)})
					if result["data"] != nil {
						ds := gripql.GetDataFieldTypes(result["data"].(map[string]interface{}))
						util.MergeMaps(schema, ds)
					}
				}
			}
			if err := iter.Close(); err != nil {
				log.WithFields(log.Fields{"graph": graph, "label": label, "error": err}).Error("getEdgeSchema: MongoDB: iter error")
				return err
			}

			fromToPairs = ma.resolveLabels(graph, fromToPairs)
			from := fromToPairs.GetFrom()
			to := fromToPairs.GetTo()

			for j := 0; j < len(from); j++ {
				eSchema := &gripql.Edge{Label: label, From: from[j], To: to[j], Data: protoutil.AsStruct(schema)}
				schemaChan <- eSchema
			}
			log.WithFields(log.Fields{"graph": graph, "label": label}).Debug("getEdgeSchema: Finished schema build")

			return nil
		})
	}

	output := []*gripql.Edge{}
	done := make(chan interface{})
	go func() {
		for s := range schemaChan {
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
