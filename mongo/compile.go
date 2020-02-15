package mongo

import (
	"fmt"
	"strings"

	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/protoutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// Pipeline a set of runnable query operations
type Pipeline struct {
	procs     []gdbi.Processor
	dataType  gdbi.DataType
	markTypes map[string]gdbi.DataType
}

// DataType return the datatype
func (pipe *Pipeline) DataType() gdbi.DataType {
	return pipe.dataType
}

// MarkTypes get the mark types
func (pipe *Pipeline) MarkTypes() map[string]gdbi.DataType {
	return pipe.markTypes
}

// Processors gets the list of processors
func (pipe *Pipeline) Processors() []gdbi.Processor {
	return pipe.procs
}

// Compiler is a mongo specific compiler that works with default graph interface
type Compiler struct {
	db *Graph
}

// NewCompiler creates a new compiler that runs using the provided GraphInterface
func NewCompiler(db *Graph) gdbi.Compiler {
	return &Compiler{db: db}
}

// Compile compiles a set of graph traversal statements into a mongo aggregation pipeline
func (comp *Compiler) Compile(stmts []*gripql.GraphStatement) (gdbi.Pipeline, error) {
	procs := []gdbi.Processor{}
	query := mongo.Pipeline{}
	startCollection := ""
	lastType := gdbi.NoData
	markTypes := map[string]gdbi.DataType{}
	aggTypes := map[string]*gripql.Aggregate{}
	vertCol := fmt.Sprintf("%s_vertices", comp.db.graph)
	edgeCol := fmt.Sprintf("%s_edges", comp.db.graph)

	stmts = core.Flatten(stmts)

	for _, gs := range stmts {
		switch stmt := gs.GetStatement().(type) {
		case *gripql.GraphStatement_V:
			if lastType != gdbi.NoData {
				return &Pipeline{}, fmt.Errorf(`"V" statement is only valid at the beginning of the traversal`)
			}
			startCollection = vertCol
			ids := protoutil.AsStringList(stmt.V)
			if len(ids) > 0 {
				query = append(query, bson.D{primitive.E{Key: "$match", Value: bson.M{"_id": bson.M{"$in": ids}}}})
			}
			lastType = gdbi.VertexData

		case *gripql.GraphStatement_E:
			if lastType != gdbi.NoData {
				return &Pipeline{}, fmt.Errorf(`"E" statement is only valid at the beginning of the traversal`)
			}
			startCollection = edgeCol
			ids := protoutil.AsStringList(stmt.E)
			if len(ids) > 0 {
				query = append(query, bson.D{primitive.E{Key: "$match", Value: bson.M{"_id": bson.M{"$in": ids}}}})
			}
			lastType = gdbi.EdgeData

		case *gripql.GraphStatement_Search:
			if lastType != gdbi.NoData {
				return &Pipeline{}, fmt.Errorf(`"Index" statement is only valid at the beginning of the traversal`)
			}
			startCollection = vertCol
			reg := fmt.Sprintf("^%s", stmt.Search.Term)
			if len(stmt.Search.Fields) == 1 {
				field := convertPath(stmt.Search.Fields[0])
				query = append(query, bson.D{primitive.E{Key: "$match", Value: bson.M{field: bson.M{"$regex": reg}}}})
			} else {
				a := []interface{}{}
				for _, i := range stmt.Search.Fields {
					field := convertPath(i)
					a = append(a, bson.M{field: bson.M{"$regex": reg}})
				}
				query = append(query, bson.D{primitive.E{Key: "$match", Value: bson.M{"$or": a}}})
			}
			lastType = gdbi.VertexData

		case *gripql.GraphStatement_In, *gripql.GraphStatement_InV:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"in" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			labels := append(protoutil.AsStringList(gs.GetIn()), protoutil.AsStringList(gs.GetInV())...)
			if lastType == gdbi.VertexData {
				query = append(query,
					bson.D{primitive.E{
						Key: "$lookup", Value: bson.M{
							"from":         edgeCol,
							"localField":   "_id",
							"foreignField": "to",
							"as":           "dst",
						},
					}},
				)
				query = append(query, bson.D{primitive.E{Key: "$unwind", Value: "$dst"}})
				query = append(query, bson.D{primitive.E{Key: "$project", Value: bson.M{
					"_id":   "$dst._id",
					"label": "$dst.label",
					"data":  "$dst.data",
					"to":    "$dst.to",
					"from":  "$dst.from",
					"marks": "$marks",
				}}})
			}
			if len(labels) > 0 {
				query = append(query, bson.D{primitive.E{Key: "$match", Value: bson.M{"label": bson.M{"$in": labels}}}})
			}
			query = append(query,
				bson.D{primitive.E{
					Key: "$lookup", Value: bson.M{
						"from":         vertCol,
						"localField":   "from",
						"foreignField": "_id",
						"as":           "dst",
					},
				}},
			)
			query = append(query, bson.D{primitive.E{Key: "$unwind", Value: "$dst"}})
			query = append(query, bson.D{primitive.E{Key: "$project", Value: bson.M{
				"_id":   "$dst._id",
				"label": "$dst.label",
				"data":  "$dst.data",
				"marks": "$marks",
			}}})
			lastType = gdbi.VertexData

		case *gripql.GraphStatement_Out, *gripql.GraphStatement_OutV:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"out" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			labels := append(protoutil.AsStringList(gs.GetOut()), protoutil.AsStringList(gs.GetOutV())...)
			if lastType == gdbi.VertexData {
				query = append(query,
					bson.D{primitive.E{
						Key: "$lookup", Value: bson.M{
							"from":         edgeCol,
							"localField":   "_id",
							"foreignField": "from",
							"as":           "dst",
						},
					}},
				)
				query = append(query, bson.D{primitive.E{Key: "$unwind", Value: "$dst"}})
				query = append(query, bson.D{primitive.E{Key: "$project", Value: bson.M{
					"_id":   "$dst._id",
					"label": "$dst.label",
					"data":  "$dst.data",
					"to":    "$dst.to",
					"from":  "$dst.from",
					"marks": "$marks",
				}}})
			}
			if len(labels) > 0 {
				query = append(query, bson.D{primitive.E{Key: "$match", Value: bson.M{"label": bson.M{"$in": labels}}}})
			}
			query = append(query,
				bson.D{primitive.E{
					Key: "$lookup", Value: bson.M{
						"from":         vertCol,
						"localField":   "to",
						"foreignField": "_id",
						"as":           "dst",
					},
				}},
			)
			query = append(query, bson.D{primitive.E{Key: "$unwind", Value: "$dst"}})
			query = append(query, bson.D{primitive.E{Key: "$project", Value: bson.M{
				"_id":   "$dst._id",
				"label": "$dst.label",
				"data":  "$dst.data",
				"marks": "$marks",
			}}})
			lastType = gdbi.VertexData

		case *gripql.GraphStatement_Both, *gripql.GraphStatement_BothV:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"both" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			labels := append(protoutil.AsStringList(gs.GetBoth()), protoutil.AsStringList(gs.GetBothV())...)
			if lastType == gdbi.VertexData {
				query = append(query,
					bson.D{primitive.E{
						Key: "$lookup", Value: bson.M{
							"from": edgeCol,
							"let":  bson.M{"vid": "$_id", "marks": "$marks"},
							"pipeline": []bson.M{
								{
									"$match": bson.M{
										"$expr": bson.M{
											"$or": []bson.M{
												{"$eq": []string{"$to", "$$vid"}},
												{"$eq": []string{"$from", "$$vid"}},
											},
										},
									},
								},
								{
									"$sort": bson.M{"to": 1},
								},
							},
							"as": "dst",
						},
					}},
				)
				query = append(query, bson.D{primitive.E{Key: "$unwind", Value: "$dst"}})
				query = append(query, bson.D{primitive.E{Key: "$project", Value: bson.M{
					"_id":   "$dst._id",
					"label": "$dst.label",
					"data":  "$dst.data",
					"to":    "$dst.to",
					"from":  "$dst.from",
					"marks": "$marks",
					"vid":   "$_id",
				}}})
			}
			if len(labels) > 0 {
				query = append(query, bson.D{primitive.E{Key: "$match", Value: bson.M{"label": bson.M{"$in": labels}}}})
			}
			query = append(query,
				bson.D{primitive.E{
					Key: "$lookup", Value: bson.M{
						"from": vertCol,
						"let":  bson.M{"to": "$to", "from": "$from", "marks": "$marks", "vid": "$vid"},
						"pipeline": []bson.M{
							{
								"$match": bson.M{
									"$expr": bson.M{
										"$and": []bson.M{
											{
												"$or": []bson.M{
													{"$eq": []string{"$_id", "$$from"}},
													{"$eq": []string{"$_id", "$$to"}},
												},
											},
											{
												"$ne": []string{"$_id", "$$vid"},
											},
										},
									},
								},
							},
						},
						"as": "dst",
					},
				}},
			)
			query = append(query, bson.D{primitive.E{Key: "$unwind", Value: "$dst"}})
			query = append(query, bson.D{primitive.E{Key: "$project", Value: bson.M{
				"_id":   "$dst._id",
				"label": "$dst.label",
				"data":  "$dst.data",
				"marks": "$marks",
			}}})
			lastType = gdbi.VertexData

		case *gripql.GraphStatement_InE:
			if lastType != gdbi.VertexData {
				return &Pipeline{}, fmt.Errorf(`"inEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			query = append(query,
				bson.D{primitive.E{
					Key: "$lookup", Value: bson.M{
						"from":         edgeCol,
						"localField":   "_id",
						"foreignField": "to",
						"as":           "dst",
					},
				}},
			)
			query = append(query, bson.D{primitive.E{Key: "$unwind", Value: "$dst"}})
			query = append(query, bson.D{primitive.E{Key: "$project", Value: bson.M{
				"_id":   "$dst._id",
				"label": "$dst.label",
				"data":  "$dst.data",
				"to":    "$dst.to",
				"from":  "$dst.from",
				"marks": "$marks",
			}}})
			labels := protoutil.AsStringList(stmt.InE)
			if len(labels) > 0 {
				query = append(query, bson.D{primitive.E{Key: "$match", Value: bson.M{"label": bson.M{"$in": labels}}}})
			}
			lastType = gdbi.EdgeData

		case *gripql.GraphStatement_OutE:
			if lastType != gdbi.VertexData {
				return &Pipeline{}, fmt.Errorf(`"outEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			query = append(query,
				bson.D{primitive.E{
					Key: "$lookup", Value: bson.M{
						"from":         edgeCol,
						"localField":   "_id",
						"foreignField": "from",
						"as":           "dst",
					},
				}},
			)
			query = append(query, bson.D{primitive.E{Key: "$unwind", Value: "$dst"}})
			query = append(query, bson.D{primitive.E{Key: "$project", Value: bson.M{
				"_id":   "$dst._id",
				"label": "$dst.label",
				"data":  "$dst.data",
				"to":    "$dst.to",
				"from":  "$dst.from",
				"marks": "$marks",
			}}})
			labels := protoutil.AsStringList(stmt.OutE)
			if len(labels) > 0 {
				query = append(query, bson.D{primitive.E{Key: "$match", Value: bson.M{"label": bson.M{"$in": labels}}}})
			}
			lastType = gdbi.EdgeData

		case *gripql.GraphStatement_BothE:
			if lastType != gdbi.VertexData {
				return &Pipeline{}, fmt.Errorf(`"bothEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			query = append(query,
				bson.D{primitive.E{
					Key: "$lookup", Value: bson.M{
						"from": edgeCol,
						"let":  bson.M{"vid": "$_id", "marks": "$marks"},
						"pipeline": []bson.M{
							{
								"$match": bson.M{
									"$expr": bson.M{
										"$or": []bson.M{
											{"$eq": []string{"$to", "$$vid"}},
											{"$eq": []string{"$from", "$$vid"}},
										},
									},
								},
							},
							{
								"$sort": bson.M{"to": 1},
							},
						},
						"as": "dst",
					},
				}},
			)
			query = append(query, bson.D{primitive.E{Key: "$unwind", Value: "$dst"}})
			query = append(query, bson.D{primitive.E{Key: "$project", Value: bson.M{
				"_id":   "$dst._id",
				"label": "$dst.label",
				"data":  "$dst.data",
				"to":    "$dst.to",
				"from":  "$dst.from",
				"marks": "$marks",
			}}})
			labels := protoutil.AsStringList(stmt.BothE)
			if len(labels) > 0 {
				query = append(query, bson.D{primitive.E{Key: "$match", Value: bson.M{"label": bson.M{"$in": labels}}}})
			}
			lastType = gdbi.EdgeData

		case *gripql.GraphStatement_Has:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"has" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			whereExpr := convertHasExpression(stmt.Has, false)
			matchStmt := bson.D{primitive.E{Key: "$match", Value: whereExpr}}
			query = append(query, matchStmt)

		case *gripql.GraphStatement_HasLabel:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"hasLabel" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			labels := protoutil.AsStringList(stmt.HasLabel)
			ilabels := make([]interface{}, len(labels))
			for i, v := range labels {
				ilabels[i] = v
			}
			has := gripql.Within("_label", ilabels...)
			whereExpr := convertHasExpression(has, false)
			matchStmt := bson.D{primitive.E{Key: "$match", Value: whereExpr}}
			query = append(query, matchStmt)

		case *gripql.GraphStatement_HasId:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"hasId" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			ids := protoutil.AsStringList(stmt.HasId)
			iids := make([]interface{}, len(ids))
			for i, v := range ids {
				iids[i] = v
			}
			has := gripql.Within("_gid", iids...)
			whereExpr := convertHasExpression(has, false)
			matchStmt := bson.D{primitive.E{Key: "$match", Value: whereExpr}}
			query = append(query, matchStmt)

		case *gripql.GraphStatement_HasKey:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"hasKey" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			hasKeys := bson.M{}
			keys := protoutil.AsStringList(stmt.HasKey)
			for _, key := range keys {
				key = jsonpath.GetJSONPath(key)
				key = strings.TrimPrefix(key, "$.")
				hasKeys[key] = bson.M{"$exists": true}
			}
			query = append(query, bson.D{primitive.E{Key: "$match", Value: hasKeys}})

		case *gripql.GraphStatement_Limit:
			query = append(query,
				bson.D{primitive.E{Key: "$limit", Value: stmt.Limit}})

		case *gripql.GraphStatement_Skip:
			query = append(query,
				bson.D{primitive.E{Key: "$skip", Value: stmt.Skip}})

		case *gripql.GraphStatement_Range:
			query = append(query,
				bson.D{primitive.E{Key: "$skip", Value: stmt.Range.Start}})
			if stmt.Range.Stop != -1 {
				query = append(query,
					bson.D{primitive.E{Key: "$limit", Value: stmt.Range.Stop - stmt.Range.Start}})
			}

		case *gripql.GraphStatement_Count:
			query = append(query,
				bson.D{primitive.E{Key: "$count", Value: "count"}})
			lastType = gdbi.CountData

		case *gripql.GraphStatement_Distinct:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"distinct" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			fields := protoutil.AsStringList(stmt.Distinct)
			if len(fields) == 0 {
				fields = append(fields, "_gid")
			}
			keys := bson.M{}
			match := bson.M{}
			for _, f := range fields {
				namespace := jsonpath.GetNamespace(f)
				f = jsonpath.GetJSONPath(f)
				f = strings.TrimPrefix(f, "$.")
				if f == "gid" {
					f = "_id"
				}
				if namespace != jsonpath.Current {
					f = fmt.Sprintf("marks.%s.%s", namespace, f)
				}
				match[f] = bson.M{"$exists": true}
				k := strings.Replace(f, ".", "_", -1)
				keys[k] = "$" + f
			}
			query = append(query, bson.D{primitive.E{
				Key: "$match", Value: match,
			}})
			query = append(query, bson.D{primitive.E{
				Key: "$group", Value: bson.M{
					"_id": keys,
					"dst": bson.M{"$first": "$$ROOT"},
				},
			},
			})
			switch lastType {
			case gdbi.VertexData:
				query = append(query, bson.D{primitive.E{Key: "$project", Value: bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "marks": "$dst.marks"}}})
			case gdbi.EdgeData:
				query = append(query, bson.D{primitive.E{Key: "$project", Value: bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "to": "$dst.to", "from": "$dst.from", "marks": "$dst.marks"}}})
			}

		case *gripql.GraphStatement_As:
			if lastType == gdbi.NoData {
				return &Pipeline{}, fmt.Errorf(`"as" statement is not valid at the beginning of a traversal`)
			}
			if stmt.As == "" {
				return &Pipeline{}, fmt.Errorf(`"as" statement cannot have an empty name`)
			}
			if err := gripql.ValidateFieldName(stmt.As); err != nil {
				return &Pipeline{}, fmt.Errorf(`"as" statement invalid; %v`, err)
			}
			if stmt.As == jsonpath.Current {
				return &Pipeline{}, fmt.Errorf(`"as" statement invalid; uses reserved name %s`, jsonpath.Current)
			}
			markTypes[stmt.As] = lastType
			query = append(query, bson.D{primitive.E{Key: "$addFields", Value: bson.M{"marks": bson.M{stmt.As: "$$ROOT"}}}})

		case *gripql.GraphStatement_Select:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"select" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			switch len(stmt.Select.Marks) {
			case 0:
				return &Pipeline{}, fmt.Errorf(`"select" statement has an empty list of mark names`)
			case 1:
				mark := "$marks." + stmt.Select.Marks[0]
				switch markTypes[stmt.Select.Marks[0]] {
				case gdbi.VertexData:
					query = append(query, bson.D{primitive.E{Key: "$project", Value: bson.M{"_id": mark + "._id", "label": mark + ".label", "data": mark + ".data", "marks": 1}}})
					lastType = gdbi.VertexData
				case gdbi.EdgeData:
					query = append(query, bson.D{primitive.E{Key: "$project", Value: bson.M{"_id": mark + "._id", "label": mark + ".label", "from": mark + ".from", "to": mark + ".to", "data": mark + ".data", "marks": 1}}})
					lastType = gdbi.EdgeData
				}
			default:
				selection := bson.M{}
				for _, mark := range stmt.Select.Marks {
					selection["marks."+mark] = 1
				}
				query = append(query, bson.D{primitive.E{Key: "$project", Value: selection}})
				lastType = gdbi.SelectionData
			}

		case *gripql.GraphStatement_Render:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"render" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			procs = append(procs, &core.Render{Template: protoutil.UnWrapValue(stmt.Render)})
			lastType = gdbi.RenderData

		case *gripql.GraphStatement_Fields:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"fields" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			fields := protoutil.AsStringList(stmt.Fields)
			includeFields := []string{}
			excludeFields := []string{}
		SelectLoop:
			for _, f := range fields {
				exclude := false
				if strings.HasPrefix(f, "-") {
					exclude = true
					f = strings.TrimPrefix(f, "-")
				}
				namespace := jsonpath.GetNamespace(f)
				if namespace != jsonpath.Current {
					log.Errorf("FieldsProcessor: only can select field from current traveler")
					continue SelectLoop
				}
				f = jsonpath.GetJSONPath(f)
				f = strings.TrimPrefix(f, "$.")
				if exclude {
					excludeFields = append(excludeFields, f)
				} else {
					includeFields = append(includeFields, f)
				}
			}

			fieldSelect := bson.M{}
			for _, v := range excludeFields {
				fieldSelect[v] = 0
			}

			if len(includeFields) > 0 || len(excludeFields) == 0 {
				fieldSelect = bson.M{"_id": 1, "label": 1, "from": 1, "to": 1, "marks": 1}
				for _, v := range excludeFields {
					switch v {
					case "gid":
						fieldSelect["_id"] = 0
					case "label":
						delete(fieldSelect, "label")
					case "from":
						delete(fieldSelect, "from")
					case "to":
						delete(fieldSelect, "to")
					}
				}
				for _, v := range includeFields {
					fieldSelect[v] = 1
				}
			}

			query = append(query, bson.D{primitive.E{Key: "$project", Value: fieldSelect}})

		case *gripql.GraphStatement_Aggregate:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"aggregate" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			aggNames := make(map[string]interface{})
			for _, a := range stmt.Aggregate.Aggregations {
				if _, ok := aggNames[a.Name]; ok {
					return &Pipeline{}, fmt.Errorf("duplicate aggregation name '%s' found; all aggregations must have a unique name", a.Name)
				}
			}
			aggs := bson.M{}
			for _, a := range stmt.Aggregate.Aggregations {
				switch a.Aggregation.(type) {
				case *gripql.Aggregate_Term:
					agg := a.GetTerm()
					field := jsonpath.GetJSONPath(agg.Field)
					field = strings.TrimPrefix(field, "$.")
					if field == "gid" {
						field = "_id"
					}
					stmt := []bson.M{
						{
							"$match": bson.M{
								field: bson.M{"$exists": true},
							},
						},
						{
							"$sortByCount": "$" + field,
						},
					}
					if agg.Size > 0 {
						stmt = append(stmt, bson.M{"$limit": agg.Size})
					}
					aggTypes[a.Name] = a
					aggs[a.Name] = stmt

				case *gripql.Aggregate_Histogram:
					agg := a.GetHistogram()
					field := jsonpath.GetJSONPath(agg.Field)
					field = strings.TrimPrefix(field, "$.")
					stmt := []bson.M{
						{
							"$match": bson.M{
								field: bson.M{"$exists": true},
							},
						},
						{
							"$group": bson.M{
								"_id": bson.M{
									"$multiply": []interface{}{agg.Interval, bson.M{"$floor": bson.M{"$divide": []interface{}{"$" + field, agg.Interval}}}},
								},
								"count": bson.M{"$sum": 1},
							},
						},
						{
							"$sort": bson.M{"_id": 1},
						},
					}
					aggTypes[a.Name] = a
					aggs[a.Name] = stmt

				case *gripql.Aggregate_Percentile:
					agg := a.GetPercentile()
					field := jsonpath.GetJSONPath(agg.Field)
					field = strings.TrimPrefix(field, "$.")
					stmt := []bson.M{
						{
							"$match": bson.M{
								field: bson.M{"$exists": true},
							},
						},
						{
							"$sort": bson.M{field: 1},
						},
						{
							"$group": bson.M{
								"_id":    "null",
								"values": bson.M{"$push": "$" + field},
							},
						},
					}
					percentiles := []interface{}{}
					for _, p := range agg.Percents {
						pName := strings.Replace(fmt.Sprintf("%v", p), ".", "_", -1)
						percentile := bson.M{}
						percentile["_id"] = pName
						percentile["count"] = percentileCalc(p)
						percentiles = append(percentiles, percentile)
					}
					stmt = append(stmt, bson.M{"$project": bson.M{"results": percentiles}})
					stmt = append(stmt, bson.M{"$unwind": "$results"})
					stmt = append(stmt, bson.M{"$project": bson.M{"_id": "$results._id", "count": "$results.count"}})
					aggTypes[a.Name] = a
					aggs[a.Name] = stmt

				default:
					return &Pipeline{}, fmt.Errorf("%s uses an unknown aggregation type", a.Name)
				}
			}
			query = append(query, bson.D{primitive.E{Key: "$facet", Value: aggs}})
			lastType = gdbi.AggregationData

		default:
			return &Pipeline{}, fmt.Errorf("unknown statement type")
		}
	}

	//log.Info("%s", query)
	// query must be less than 16MB limit
	bsonSize, err := bson.Marshal(bson.M{"pipeline": query})
	if err != nil {
		return &Pipeline{}, fmt.Errorf("failed to marshal query into BSON: %s", err)
	}
	if len(bsonSize) > 16000000 {
		return &Pipeline{}, fmt.Errorf("BSON input query size: %v greater than max (16MB)", len(bsonSize))
	}

	procs = append([]gdbi.Processor{&Processor{comp.db, startCollection, query, lastType, markTypes, aggTypes}}, procs...)
	return &Pipeline{procs, lastType, markTypes}, nil
}
