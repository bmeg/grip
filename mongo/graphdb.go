package mongo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/timestamp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
	client   *mongo.Client
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
	clientOpts := options.Client()

	clientOpts.SetAppName("grip")
	clientOpts.SetConnectTimeout(1 * time.Minute)
	if conf.Username != "" || conf.Password != "" {
		cred := options.Credential{Username: conf.Username, Password: conf.Password}
		clientOpts.SetAuth(cred)
	}
	clientOpts.SetRetryReads(true)
	clientOpts.SetRetryWrites(true)
	clientOpts.ApplyURI(conf.URL)

	/*
		dialinfo := &mgo.DialInfo{
			Addrs:         []string{conf.URL},
			Timeout:       1 * time.Minute,
			Database:      conf.DBName,
			Username:      conf.Username,
			Password:      conf.Password,
			AppName:       "grip",
			ReadTimeout:   10 * time.Minute,
			WriteTimeout:  10 * time.Minute,
			PoolLimit:     4096,
			PoolTimeout:   0,
			MinPoolSize:   10,
			MaxIdleTimeMS: 600000, // 10 minutes
		}
	*/

	client, err := mongo.NewClient(clientOpts)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	/*
		b, _ := session.BuildInfo()
		if !b.VersionAtLeast(3, 6) {
			session.Close()
			return nil, fmt.Errorf("requires mongo 3.6 or later")
		}
	*/

	if conf.BatchSize == 0 {
		conf.BatchSize = 1000
	}
	db := &GraphDB{database: database, conf: conf, client: client, ts: &ts}
	for _, g := range db.ListGraphs() {
		g := g
		db.ts.Touch(g)
		/*
			go func() {
				err := db.setupIndices(g)
				if err != nil {
					log.WithFields(log.Fields{"error": err, "graph": g}).Error("Setting up indices")
				}
			}()
		*/
	}
	return db, nil
}

// Close the connection
func (ma *GraphDB) Close() error {
	ma.client = nil
	return nil
}

// VertexCollection returns a *mgo.Collection
func (ma *GraphDB) VertexCollection(graph string) *mongo.Collection {
	return ma.client.Database(ma.database).Collection(fmt.Sprintf("%s_vertices", graph))
}

// EdgeCollection returns a *mgo.Collection
func (ma *GraphDB) EdgeCollection(graph string) *mongo.Collection {
	return ma.client.Database(ma.database).Collection(fmt.Sprintf("%s_edges", graph))
}

// AddGraph creates a new graph named `graph`
func (ma *GraphDB) AddGraph(graph string) error {
	err := gripql.ValidateGraphName(graph)
	if err != nil {
		return err
	}
	defer ma.ts.Touch(graph)
	return AddMongoGraph(ma.client, ma.database, graph)
}

// DeleteGraph deletes `graph`
func (ma *GraphDB) DeleteGraph(graph string) error {
	defer ma.ts.Touch(graph)

	g := ma.client.Database(ma.database).Collection("graphs")
	v := ma.VertexCollection(graph)
	e := ma.EdgeCollection(graph)

	verr := v.Drop(context.TODO())
	if verr != nil {
		log.WithFields(log.Fields{"error": verr, "graph": graph}).Error("DeleteGraph: MongoDB: dropping vertex collection")
	}
	eerr := e.Drop(context.TODO())
	if eerr != nil {
		log.WithFields(log.Fields{"error": eerr, "graph": graph}).Error("DeleteGraph: MongoDB: dropping edge collection")
	}
	_, gerr := g.DeleteOne(context.TODO(), bson.M{"_id": graph})
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
	out := make([]string, 0, 100)
	g := ma.client.Database(ma.database).Collection("graphs")

	cursor, err := g.Find(context.TODO(), bson.M{})
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("ListGraphs: MongoDB: list error")
		return nil
	}
	result := map[string]interface{}{}
	for cursor.Next(context.TODO()) {
		cursor.Decode(&result)
		out = append(out, result["_id"].(string))
	}
	if err := cursor.Close(context.TODO()); err != nil {
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
