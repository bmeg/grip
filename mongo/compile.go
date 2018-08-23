package mongo

import (
	"fmt"
	"strings"

	"github.com/bmeg/grip/aql"
	"github.com/bmeg/grip/engine/core"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/protoutil"
	"github.com/globalsign/mgo/bson"
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
func (comp *Compiler) Compile(stmts []*aql.GraphStatement) (gdbi.Pipeline, error) {
	procs := []gdbi.Processor{}
	query := []bson.M{}
	startCollection := ""
	lastType := gdbi.NoData
	markTypes := map[string]gdbi.DataType{}
	aggTypes := map[string]aggType{}
	vertCol := fmt.Sprintf("%s_vertices", comp.db.graph)
	edgeCol := fmt.Sprintf("%s_edges", comp.db.graph)

	stmts = core.Flatten(stmts)

	for _, gs := range stmts {
		switch stmt := gs.GetStatement().(type) {
		case *aql.GraphStatement_V:
			if lastType != gdbi.NoData {
				return &Pipeline{}, fmt.Errorf(`"V" statement is only valid at the beginning of the traversal`)
			}
			startCollection = vertCol
			ids := protoutil.AsStringList(stmt.V)
			if len(ids) > 0 {
				query = append(query, bson.M{"$match": bson.M{"_id": bson.M{"$in": ids}}})
			}
			lastType = gdbi.VertexData

		case *aql.GraphStatement_E:
			if lastType != gdbi.NoData {
				return &Pipeline{}, fmt.Errorf(`"E" statement is only valid at the beginning of the traversal`)
			}
			startCollection = edgeCol
			ids := protoutil.AsStringList(stmt.E)
			if len(ids) > 0 {
				query = append(query, bson.M{"$match": bson.M{"_id": bson.M{"$in": ids}}})
			}
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_In:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"in" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			labels := protoutil.AsStringList(stmt.In)
			if lastType == gdbi.VertexData {
				query = append(query,
					bson.M{
						"$lookup": bson.M{
							"from": edgeCol,
							"let":  bson.M{"vid": "$_id", "marks": "$marks"},
							"pipeline": []bson.M{
								{
									"$match": bson.M{
										"$expr": bson.M{
											"$eq": []string{"$to", "$$vid"},
										},
									},
								},
								{
									"$addFields": bson.M{"marks": "$$marks"},
								},
							},
							"as": "dst",
						},
					},
				)
				query = append(query, bson.M{"$unwind": "$dst"})
				query = append(query, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "to": "$dst.to", "from": "$dst.from", "marks": "$dst.marks"}})
			}
			if len(labels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"label": bson.M{"$in": labels}}})
			}
			query = append(query,
				bson.M{
					"$lookup": bson.M{
						"from": vertCol,
						"let":  bson.M{"from": "$from", "marks": "$marks"},
						"pipeline": []bson.M{
							{
								"$match": bson.M{
									"$expr": bson.M{
										"$eq": []string{"$_id", "$$from"},
									},
								},
							},
							{
								"$addFields": bson.M{"marks": "$$marks"},
							},
						},
						"as": "dst",
					},
				},
			)
			query = append(query, bson.M{"$unwind": "$dst"})
			query = append(query, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "marks": "$dst.marks"}})
			lastType = gdbi.VertexData

		case *aql.GraphStatement_Out:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"out" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			labels := protoutil.AsStringList(stmt.Out)
			if lastType == gdbi.VertexData {
				query = append(query,
					bson.M{
						"$lookup": bson.M{
							"from": edgeCol,
							"let":  bson.M{"vid": "$_id", "marks": "$marks"},
							"pipeline": []bson.M{
								{
									"$match": bson.M{
										"$expr": bson.M{
											"$eq": []string{"$from", "$$vid"},
										},
									},
								},
								{
									"$addFields": bson.M{"marks": "$$marks"},
								},
							},
							"as": "dst",
						},
					},
				)
				query = append(query, bson.M{"$unwind": "$dst"})
				query = append(query, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "to": "$dst.to", "from": "$dst.from", "marks": "$dst.marks"}})
			}
			if len(labels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"label": bson.M{"$in": labels}}})
			}
			query = append(query,
				bson.M{
					"$lookup": bson.M{
						"from": vertCol,
						"let":  bson.M{"to": "$to", "marks": "$marks"},
						"pipeline": []bson.M{
							{
								"$match": bson.M{
									"$expr": bson.M{
										"$eq": []string{"$_id", "$$to"},
									},
								},
							},
							{
								"$addFields": bson.M{"marks": "$$marks"},
							},
						},
						"as": "dst",
					},
				},
			)
			query = append(query, bson.M{"$unwind": "$dst"})
			query = append(query, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "marks": "$dst.marks"}})
			lastType = gdbi.VertexData

		case *aql.GraphStatement_Both:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"both" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			labels := protoutil.AsStringList(stmt.Both)
			if lastType == gdbi.VertexData {
				query = append(query,
					bson.M{
						"$lookup": bson.M{
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
									"$addFields": bson.M{"vid": "$$vid", "marks": "$$marks"},
								},
								{
									"$sort": bson.M{"to": 1},
								},
							},
							"as": "dst",
						},
					},
				)
				query = append(query, bson.M{"$unwind": "$dst"})
				query = append(query, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "to": "$dst.to", "from": "$dst.from", "marks": "$dst.marks", "vid": "$dst.vid"}})
			}
			if len(labels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"label": bson.M{"$in": labels}}})
			}
			query = append(query,
				bson.M{
					"$lookup": bson.M{
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
							{
								"$addFields": bson.M{"marks": "$$marks"},
							},
						},
						"as": "dst",
					},
				},
			)
			query = append(query, bson.M{"$unwind": "$dst"})
			query = append(query, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "marks": "$dst.marks"}})
			lastType = gdbi.VertexData

		case *aql.GraphStatement_InEdge:
			if lastType != gdbi.VertexData {
				return &Pipeline{}, fmt.Errorf(`"inEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			query = append(query,
				bson.M{
					"$lookup": bson.M{
						"from": edgeCol,
						"let":  bson.M{"vid": "$_id", "marks": "$marks"},
						"pipeline": []bson.M{
							{
								"$match": bson.M{
									"$expr": bson.M{
										"$eq": []string{"$to", "$$vid"},
									},
								},
							},
							{
								"$addFields": bson.M{"marks": "$$marks"},
							},
						},
						"as": "dst",
					},
				},
			)
			query = append(query, bson.M{"$unwind": "$dst"})
			query = append(query, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "to": "$dst.to", "from": "$dst.from", "marks": "$dst.marks"}})
			labels := protoutil.AsStringList(stmt.InEdge)
			if len(labels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"label": bson.M{"$in": labels}}})
			}
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_OutEdge:
			if lastType != gdbi.VertexData {
				return &Pipeline{}, fmt.Errorf(`"outEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			query = append(query,
				bson.M{
					"$lookup": bson.M{
						"from": edgeCol,
						"let":  bson.M{"vid": "$_id", "marks": "$marks"},
						"pipeline": []bson.M{
							{
								"$match": bson.M{
									"$expr": bson.M{
										"$eq": []string{"$from", "$$vid"},
									},
								},
							},
							{
								"$addFields": bson.M{"marks": "$$marks"},
							},
						},
						"as": "dst",
					},
				},
			)
			query = append(query, bson.M{"$unwind": "$dst"})
			query = append(query, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "to": "$dst.to", "from": "$dst.from", "marks": "$dst.marks"}})
			labels := protoutil.AsStringList(stmt.OutEdge)
			if len(labels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"label": bson.M{"$in": labels}}})
			}
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_BothEdge:
			if lastType != gdbi.VertexData {
				return &Pipeline{}, fmt.Errorf(`"bothEdge" statement is only valid for the vertex type not: %s`, lastType.String())
			}
			query = append(query,
				bson.M{
					"$lookup": bson.M{
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
								"$addFields": bson.M{"marks": "$$marks"},
							},
							{
								"$sort": bson.M{"to": 1},
							},
						},
						"as": "dst",
					},
				},
			)
			query = append(query, bson.M{"$unwind": "$dst"})
			query = append(query, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "to": "$dst.to", "from": "$dst.from", "marks": "$dst.marks"}})
			labels := protoutil.AsStringList(stmt.BothEdge)
			if len(labels) > 0 {
				query = append(query, bson.M{"$match": bson.M{"label": bson.M{"$in": labels}}})
			}
			lastType = gdbi.EdgeData

		case *aql.GraphStatement_Where:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"where" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			whereExpr := convertWhereExpression(stmt.Where, false)
			matchStmt := bson.M{"$match": whereExpr}
			query = append(query, matchStmt)

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
				return &Pipeline{}, fmt.Errorf(`"distinct" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			fields := protoutil.AsStringList(stmt.Distinct)
			if len(fields) == 0 {
				fields = append(fields, "_gid")
			}
			keys := bson.M{}
			match := bson.M{}
			for _, f := range fields {
				f = jsonpath.GetJSONPath(f)
				f = strings.TrimPrefix(f, "$.")
				if f == "gid" {
					f = "_id"
				}
				namespace := jsonpath.GetNamespace(f)
				if namespace != jsonpath.Current {
					f = fmt.Sprintf("marks.%s.%s", namespace, f)
				}
				match[f] = bson.M{"$exists": true}
				k := strings.Replace(f, ".", "_", -1)
				keys[k] = "$" + f
			}
			distinct := []bson.M{
				{
					"$match": match,
				},
				{
					"$group": bson.M{
						"_id": keys,
						"dst": bson.M{"$first": "$$ROOT"},
					},
				},
			}
			switch lastType {
			case gdbi.VertexData:
				distinct = append(distinct, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "marks": "$dst.marks"}})
			case gdbi.EdgeData:
				distinct = append(distinct, bson.M{"$project": bson.M{"_id": "$dst._id", "label": "$dst.label", "data": "$dst.data", "to": "$dst.to", "from": "$dst.from", "marks": "$dst.marks"}})
			}
			query = append(query, distinct...)

		case *aql.GraphStatement_Mark:
			if lastType == gdbi.NoData {
				return &Pipeline{}, fmt.Errorf(`"mark" statement is not valid at the beginning of a traversal`)
			}
			if stmt.Mark == "" {
				return &Pipeline{}, fmt.Errorf(`"mark" statement cannot have an empty name`)
			}
			if err := aql.ValidateFieldName(stmt.Mark); err != nil {
				return &Pipeline{}, fmt.Errorf(`"mark" statement invalid; %v`, err)
			}
			if stmt.Mark == jsonpath.Current {
				return &Pipeline{}, fmt.Errorf(`"mark" statement invalid; uses reserved name %s`, jsonpath.Current)
			}
			markTypes[stmt.Mark] = lastType
			query = append(query, bson.M{"$addFields": bson.M{"marks": bson.M{stmt.Mark: "$$ROOT"}}})

		case *aql.GraphStatement_Select:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"select" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			if len(stmt.Select.Marks) == 0 {
				return &Pipeline{}, fmt.Errorf(`"select" statement has an empty list of mark names`)
			}
			selection := bson.M{}
			for _, mark := range stmt.Select.Marks {
				selection["marks."+mark] = 1
			}
			query = append(query, bson.M{"$project": selection})
			lastType = gdbi.SelectionData

		case *aql.GraphStatement_Render:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"render" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			procs = append(procs, &core.Render{Template: protoutil.UnWrapValue(stmt.Render)})
			lastType = gdbi.RenderData

		case *aql.GraphStatement_Fields:
			if lastType != gdbi.VertexData && lastType != gdbi.EdgeData {
				return &Pipeline{}, fmt.Errorf(`"fields" statement is only valid for edge or vertex types not: %s`, lastType.String())
			}
			fields := protoutil.AsStringList(stmt.Fields)
			fieldSelect := bson.M{"_id": 0}
			for _, f := range fields {
				namespace := jsonpath.GetNamespace(f)
				f = jsonpath.GetJSONPath(f)
				f = strings.TrimPrefix(f, "$.")
				switch f {
				case "gid":
					f = "_id"
					if namespace != jsonpath.Current {
						f = "marks." + namespace + "." + f
					}
					fieldSelect[f] = 1
				default:
					if namespace != jsonpath.Current {
						f = "marks." + namespace + "." + f
					}
					fieldSelect[f] = bson.M{"$ifNull": []interface{}{"$" + f, nil}}
				}
			}
			query = append(query, bson.M{"$project": fieldSelect})

		case *aql.GraphStatement_Aggregate:
			if lastType != gdbi.VertexData {
				return &Pipeline{}, fmt.Errorf(`"aggregate" statement is only valid for vertex types not: %s`, lastType.String())
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
				case *aql.Aggregate_Term:
					agg := a.GetTerm()
					field := jsonpath.GetJSONPath(agg.Field)
					field = strings.TrimPrefix(field, "$.")
					stmt := []bson.M{
						{
							"$match": bson.M{
								"label": agg.Label,
								field:   bson.M{"$exists": true},
							},
						},
						{
							"$sortByCount": "$" + field,
						},
					}
					if agg.Size > 0 {
						stmt = append(stmt, bson.M{"$limit": agg.Size})
					}
					aggTypes[a.Name] = termAgg
					aggs[a.Name] = stmt

				case *aql.Aggregate_Histogram:
					agg := a.GetHistogram()
					field := jsonpath.GetJSONPath(agg.Field)
					field = strings.TrimPrefix(field, "$.")
					stmt := []bson.M{
						{
							"$match": bson.M{
								"label": agg.Label,
								field:   bson.M{"$exists": true},
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
					aggTypes[a.Name] = histogramAgg
					aggs[a.Name] = stmt

				case *aql.Aggregate_Percentile:
					agg := a.GetPercentile()
					field := jsonpath.GetJSONPath(agg.Field)
					field = strings.TrimPrefix(field, "$.")
					stmt := []bson.M{
						{
							"$match": bson.M{
								"label": agg.Label,
								field:   bson.M{"$exists": true},
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
					aggTypes[a.Name] = percentileAgg
					aggs[a.Name] = stmt

				default:
					return &Pipeline{}, fmt.Errorf("%s uses an unknown aggregation type", a.Name)
				}
			}
			query = append(query, bson.M{"$facet": aggs})
			lastType = gdbi.AggregationData

		default:
			return &Pipeline{}, fmt.Errorf("unknown statement type")
		}
	}

	procs = append([]gdbi.Processor{&Processor{comp.db, startCollection, query, lastType, markTypes, aggTypes}}, procs...)
	return &Pipeline{procs, lastType, markTypes}, nil
}
