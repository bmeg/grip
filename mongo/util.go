package mongo

import (
  "fmt"
  "context"
  "go.mongodb.org/mongo-driver/bson"
  "go.mongodb.org/mongo-driver/mongo/options"
  mgo "go.mongodb.org/mongo-driver/mongo"
)

func GetVertexCollection(session *mgo.Client, database string, graph string) *mgo.Collection {
	return session.Database(database).Collection(fmt.Sprintf("%s_vertices", graph))
}

func GetEdgeCollection(session *mgo.Client, database string, graph string) *mgo.Collection {
	return session.Database(database).Collection(fmt.Sprintf("%s_edges", graph))
}

func AddMongoGraph(client *mgo.Client, database string, graph string) error {
	graphs := client.Database(database).Collection("graphs")
	_, err := graphs.InsertOne(context.Background(), bson.M{"_id": graph})
	if err != nil {
		return fmt.Errorf("failed to insert graph %s: %v", graph, err)
	}

	e := GetEdgeCollection(client, database, graph)
	eiv := e.Indexes()
	_, err = eiv.CreateOne(
		context.Background(),
		mgo.IndexModel{
			Keys: []string{"from"},
			Options: options.Index().SetUnique(false).SetSparse(false).SetBackground(true),
	})
	if err != nil {
		return fmt.Errorf("failed create index for graph %s: %v", graph, err)
	}

	_, err = eiv.CreateOne(
		context.Background(),
		mgo.IndexModel{
			Keys: []string{"to"},
			Options: options.Index().SetUnique(false).SetSparse(false).SetBackground(true),
		})
	if err != nil {
		return fmt.Errorf("failed create index for graph %s: %v", graph, err)
	}

	_, err = eiv.CreateOne(
		context.Background(),
		mgo.IndexModel{
			Keys: []string{"label"},
			Options: options.Index().SetUnique(false).SetSparse(false).SetBackground(true),
		})
	if err != nil {
		return fmt.Errorf("failed create index for graph %s: %v", graph, err)
	}

	v := GetVertexCollection(client, database, graph)
	viv := v.Indexes()
	_, err = viv.CreateOne(
		context.Background(),
		mgo.IndexModel{
			Keys: []string{"label"},
			Options: options.Index().SetUnique(false).SetSparse(false).SetBackground(true),
		})
	if err != nil {
		return fmt.Errorf("failed create index for graph %s: %v", graph, err)
	}
	return nil
}
