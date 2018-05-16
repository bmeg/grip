package mongo

import (
	"context"
	"fmt"
	"log"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/engine/core"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/jsonpath"
	"github.com/bmeg/arachne/protoutil"
	"gopkg.in/mgo.v2/bson"
)

type MongoCompiler struct {
	db       *Graph
	compiler gdbi.Compiler
}

func NewCompiler(db *Graph) gdbi.Compiler {
	return &MongoCompiler{db: db, compiler: core.NewCompiler(db)}
}

type MongoPipeline struct {
	db              *Graph
	startCollection string
	query           []bson.M
	dataType        gdbi.DataType
	markTypes       map[string]gdbi.DataType
}

func (comp *MongoCompiler) Compile(stmts []*aql.GraphStatement, workDir string) (gdbi.Pipeline, error) {
	query := []bson.M{}
	startCollection := ""
	lastType := gdbi.NoData
	markTypes := map[string]gdbi.DataType{}

	vertCol := fmt.Sprintf("%s_vertices", comp.db.graph)
	edgeCol := fmt.Sprintf("%s_edges", comp.db.graph)

	for _, gs := range stmts {
		switch stmt := gs.GetStatement().(type) {
		case *aql.GraphStatement_V:
			if lastType != gdbi.NoData {
				return &MongoPipeline{}, fmt.Errorf(`"V" statement is only valid at the beginning of the traversal`)
			}
			startCollection = vertCol
			ids := protoutil.AsStringList(stmt.V)
			if len(ids) > 0 {
				query = append(query, bson.M{"$match": bson.M{fieldGid: bson.M{"$in": ids}}})
			}
			lastType = gdbi.VertexData

		case *aql.GraphStatement_E:
			if lastType != gdbi.NoData {
				return &MongoPipeline{}, fmt.Errorf(`"E" statement is only valid at the beginning of the traversal`)
			}
			startCollection = edgeCol
			ids := protoutil.AsStringList(stmt.E)
			if len(ids) > 0 {
				query = append(query, bson.M{"$match": bson.M{fieldGid: bson.M{"$in": ids}}})
			}
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_In:
			query = append(query,
				bson.M{"$lookup": bson.M{"from": edgeCol, "localField": "_id", "foreignField": "to", "as": "dst"}})
			query = append(query, bson.M{"$unwind": "$dst"})
			labels := protoutil.AsStringList(stmt.In)
			if len(labels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"dst.label": bson.M{"$in": labels}}})
			}
			query = append(query,
				bson.M{"$lookup": bson.M{"from": vertCol, "localField": "dst.from", "foreignField": "_id", "as": "dst"}})
			query = append(query, bson.M{"$unwind": "$dst"})
			query = append(query, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "mark": 1}})
			lastType = gdbi.VertexData

		case *aql.GraphStatement_Out:
			query = append(query,
				bson.M{"$lookup": bson.M{"from": edgeCol, "localField": "_id", "foreignField": "from", "as": "dst"}})
			query = append(query, bson.M{"$unwind": "$dst"})
			labels := protoutil.AsStringList(stmt.Out)
			if len(labels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"dst.label": bson.M{"$in": labels}}})
			}
			query = append(query,
				bson.M{"$lookup": bson.M{"from": vertCol, "localField": "dst.to", "foreignField": "_id", "as": "dst"}})
			query = append(query, bson.M{"$unwind": "$dst"})
			query = append(query, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "mark": 1}})
			lastType = gdbi.VertexData

		case *aql.GraphStatement_Both:
			query = append(query,
				bson.M{"$lookup": bson.M{"from": edgeCol, "localField": "_id", "foreignField": bson.M{"$in", []string{"to", "from"}}, "as": "dst"}})
			query = append(query, bson.M{"$unwind": "$dst"})
			labels := protoutil.AsStringList(stmt.In)
			if len(labels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"dst.label": bson.M{"$in": labels}}})
			}
			query = append(query,
				bson.M{"$lookup": bson.M{"from": vertCol, "localField": bson.M{"$in", []string{"dts.to", "dst.from"}}, "foreignField": "_id", "as": "dst"}})
			query = append(query, bson.M{"$unwind": "$dst"})
			query = append(query, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "mark": 1}})
			lastType = gdbi.VertexData

		case *aql.GraphStatement_InEdge:
			if lastType != gdbi.VertexData {
				return &MongoPipeline{}, fmt.Errorf(`"inEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			query = append(query,
				bson.M{"$lookup": bson.M{"from": edgeCol, "localField": "_id", "foreignField": "to", "as": "dst"}})
			query = append(query, bson.M{"$unwind": "$dst"})
			labels := protoutil.AsStringList(stmt.OutEdge)
			if len(labels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"dst.label": bson.M{"$in": labels}}})
			}
			query = append(query, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "to": "$dst.to", "from": "$dst.from"}})
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_OutEdge:
			if lastType != gdbi.VertexData {
				return &MongoPipeline{}, fmt.Errorf(`"outEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			query = append(query,
				bson.M{"$lookup": bson.M{"from": edgeCol, "localField": "_id", "foreignField": "from", "as": "dst"}})
			query = append(query, bson.M{"$unwind": "$dst"})
			labels := protoutil.AsStringList(stmt.OutEdge)
			if len(labels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"dst.label": bson.M{"$in": labels}}})
			}
			query = append(query, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "to": "$dst.to", "from": "$dst.from"}})
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_BothEdge:
			if lastType != gdbi.VertexData {
				return &MongoPipeline{}, fmt.Errorf(`"bothEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			query = append(query,
				bson.M{"$lookup": bson.M{"from": edgeCol, "localField": "_id", "foreignField": bson.M{"$in", []string{"to", "from"}}, "as": "dst"}})
			query = append(query, bson.M{"$unwind": "$dst"})
			labels := protoutil.AsStringList(stmt.OutEdge)
			if len(labels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"dst.label": bson.M{"$in": labels}}})
			}
			query = append(query, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "to": "$dst.to", "from": "$dst.from"}})
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_Where:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &MongoPipeline{}, fmt.Errorf(`"distinct" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			// TODO

		case *aql.GraphStatement_Limit:
			query = append(query,
				bson.M{"$limit": stmt.Limit})

		case *aql.GraphStatement_Offset:
			query = append(query,
				bson.M{"$skip": stmt.Offset})

		case *aql.GraphStatement_Count:
			query = append(query,
				bson.M{"$count": "count"})
			lastType = gdbi.CountData

		case *aql.GraphStatement_Distinct:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &MongoPipeline{}, fmt.Errorf(`"distinct" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			//TODO

		case *aql.GraphStatement_Mark:
			if lastType == gdbi.NoData {
				return &MongoPipeline{}, fmt.Errorf(`"mark" statement is not valid at the beginning of a traversal`)
			}
			if stmt.Mark == "" {
				return &MongoPipeline{}, fmt.Errorf(`"mark" statement cannot have an empty name`)
			}
			if err := aql.ValidateFieldName(stmt.Mark); err != nil {
				return &MongoPipeline{}, fmt.Errorf(`"mark" statement invalid; %v`, err)
			}
			if stmt.Mark == jsonpath.Current {
				return &MongoPipeline{}, fmt.Errorf(`"mark" statement invalid; uses reserved name %s`, jsonpath.Current)
			}
			markTypes[stmt.Mark] = lastType
			//TODO

		case *aql.GraphStatement_Select:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &MongoPipeline{}, fmt.Errorf(`"select" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			if len(stmt.Select.Marks) == 0 {
				return &MongoPipeline{}, fmt.Errorf(`"select" statement has an empty list of mark names`)
			}
			//TODO
			lastType = gdbi.SelectionData

		case *aql.GraphStatement_Render:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &MongoPipeline{}, fmt.Errorf(`"render" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			//TODO
			lastType = gdbi.RenderData

		case *aql.GraphStatement_Fields:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &MongoPipeline{}, fmt.Errorf(`"fields" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			//TODO

		case *aql.GraphStatement_Aggregate:
			if lastType != gdbi.VertexData {
				return &MongoPipeline{}, fmt.Errorf(`"aggregate" statement is only valid for vertex types not: %s`, lastType.String())
			}
			//TODO
			lastType = gdbi.AggregationData

		default:
			return &MongoPipeline{}, fmt.Errorf("unknown statement type")
		}
	}
	return &MongoPipeline{comp.db, startCollection, query, lastType, markTypes}, nil
}

func (pipe *MongoPipeline) DataType() gdbi.DataType {
	return pipe.dataType
}

func (pipe *MongoPipeline) MarkTypes() map[string]gdbi.DataType {
	return pipe.markTypes
}

func (pipe *MongoPipeline) Processors() []gdbi.Processor {
	return []gdbi.Processor{&MongoProcessor{pipe.db, pipe.startCollection, pipe.query, pipe.dataType, pipe.markTypes}}
}

type MongoProcessor struct {
	db              *Graph
	startCollection string
	query           []bson.M
	dataType        gdbi.DataType
	markTypes       map[string]gdbi.DataType
}

func getDataElement(data map[string]interface{}) gdbi.DataElement {
	d := gdbi.DataElement{}
	if x, ok := data["_id"]; ok {
		d.ID = x.(string)
	}
	if x, ok := data["label"]; ok {
		d.Label = x.(string)
	}
	if x, ok := data["data"]; ok {
		d.Data = x.(map[string]interface{})
	}
	if x, ok := data["to"]; ok {
		d.To = x.(string)
	}
	if x, ok := data["from"]; ok {
		d.From = x.(string)
	}
	return d
}

func (proc *MongoProcessor) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		log.Printf("Running Mongo Processor: %+v", proc.query)
		session := proc.db.ar.pool.Get()
		defer proc.db.ar.pool.Put(session)
		defer close(out)

		eCol := session.DB(proc.db.ar.database).C(proc.startCollection)
		for t := range in {
			iter := eCol.Pipe(proc.query).Iter()
			result := map[string]interface{}{}
			if proc.dataType == gdbi.CountData {
				eo := &gdbi.Traveler{
					Count: 0,
				}
				for iter.Next(&result) {
					if x, ok := result["count"]; ok {
						eo.Count = int64(x.(int))
					}
				}
				if err := iter.Err(); err != nil {
					log.Println("Mongo traversal error:", err)
					continue
				}
				out <- eo
			} else if proc.dataType == gdbi.RowData {
				for iter.Next(&result) {
					//log.Printf("result: %s", result)
					select {
					case <-ctx.Done():
						return
					default:
					}
					row := make([]gdbi.DataElement, 0, len(proc.rowTypes))
					markData := result["mark"].(map[string]interface{})
					for _, name := range proc.rowNames {
						d := getDataElement(markData[name].(map[string]interface{}))
						row = append(row, d)
					}
					out <- t.AddCurrent(&gdbi.DataElement{Row: row})
				}
				if err := iter.Err(); err != nil {
					log.Println("Mongo traversal error:", err)
					continue
				}
			} else {
				for iter.Next(&result) {
					//log.Printf("result: %s", result)
					select {
					case <-ctx.Done():
						return
					default:
					}
					d := gdbi.DataElement{}
					if x, ok := result["_id"]; ok {
						d.ID = x.(string)
					}
					if x, ok := result["label"]; ok {
						d.Label = x.(string)
					}
					if x, ok := result["data"]; ok {
						d.Data = x.(map[string]interface{})
					}
					if x, ok := result["to"]; ok {
						d.To = x.(string)
					}
					if x, ok := result["from"]; ok {
						d.From = x.(string)
					}
					out <- t.AddCurrent(&d)
				}
				if err := iter.Err(); err != nil {
					log.Println("Mongo traversal error:", err)
					continue
				}
			}
		}
	}()
	return ctx
}
