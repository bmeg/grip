package main

import (
	"fmt"
	"unicode"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

const ARG_LIMIT = "first"
const ARG_OFFSET = "offset"
const ARG_ID = "id"
const ARG_IDS = "ids"
const ARG_FILTER = "filter"

var JSONScalar = graphql.NewScalar(graphql.ScalarConfig{
	Name: "JSON",
	Serialize: func(value interface{}) interface{} {
		return fmt.Sprintf("Serialize %v", value)
	},
	ParseValue: func(value interface{}) interface{} {
		//fmt.Printf("Unmarshal JSON: %v %T\n", value, value)
		return value
	},
	ParseLiteral: func(valueAST ast.Value) interface{} {
		fmt.Printf("ParseLiteral: %#v\n", valueAST)
		/*
			switch valueAST := valueAST.(type) {
			case *ast.StringValue:
				id, _ := models.IDFromString(valueAST.Value)
				return id
			default:
				return nil
			}*/
		return nil
	},
})

// buildGraphQLSchema reads a GRIP graph schema (which is stored as a graph) and creates
// a GraphQL-GO based schema. The GraphQL-GO schema all wraps the request functions that use
// the gripql.Client to find the requested data
func buildGraphQLSchema(schema *gripql.Graph, client gripql.Client, graph string) (*graphql.Schema, error) {
	if schema == nil {
		return nil, fmt.Errorf("graphql.NewSchema error: nil gripql.Graph for graph: %s", graph)
	}
	// Build the set of objects for all vertex labels
	objectMap, err := buildObjectMap(client, graph, schema)
	fmt.Println("OBJ MAP: ", objectMap)
	if err != nil {
		return nil, fmt.Errorf("graphql.NewSchema error: %v", err)
	}

	// Build the set of objects that exist in the query structuer
	queryObj := buildQueryObject(client, graph, objectMap)
	schemaConfig := graphql.SchemaConfig{
		Query: queryObj,
	}

	// Setup the GraphQL schema based on the objects there have been created
	gqlSchema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		return nil, fmt.Errorf("graphql.NewSchema error: %v", err)
	}

	return &gqlSchema, nil
}

func buildField(x string) (*graphql.Field, error) {
	var o *graphql.Field
	switch x {
	case "NUMERIC":
		o = &graphql.Field{Type: graphql.Float}
	case "STRING":
		o = &graphql.Field{Type: graphql.String}
	case "BOOL":
		o = &graphql.Field{Type: graphql.Boolean}
	default:
		return nil, fmt.Errorf("%s does not map to a GQL type", x)
	}
	return o, nil
}

func buildSliceField(name string, s []interface{}) (*graphql.Field, error) {
	var f *graphql.Field
	var err error

	if len(s) > 0 {
		val := s[0]
		if x, ok := val.(map[string]interface{}); ok {
			f, err = buildObjectField(name, x)
		} else if x, ok := val.([]interface{}); ok {
			f, err = buildSliceField(name, x)

		} else if x, ok := val.(string); ok {
			f, err = buildField(x)
		} else {
			err = fmt.Errorf("unhandled type: %T %v", val, val)
		}

	} else {
		err = fmt.Errorf("slice is empty")
	}

	if err != nil {
		return nil, fmt.Errorf("buildSliceField error: %v", err)
	}

	return &graphql.Field{Type: graphql.NewList(f.Type)}, nil
}

// buildObjectField wraps the result of buildObject in a graphql.Field so it can be
// a child of slice of another
func buildObjectField(name string, obj map[string]interface{}) (*graphql.Field, error) {
	o, err := buildObject(name, obj)
	if err != nil {
		return nil, err
	}
	if len(o.Fields()) == 0 {
		return nil, fmt.Errorf("no fields in object")
	}
	return &graphql.Field{Type: o}, nil
}

func buildObject(name string, obj map[string]interface{}) (*graphql.Object, error) {
	objFields := graphql.Fields{}

	for key, val := range obj {
		var err error

		// handle map
		if x, ok := val.(map[string]interface{}); ok {
			// make object name parent_field
			var f *graphql.Field
			f, err = buildObjectField(name+"_"+key, x)
			if err == nil {
				objFields[key] = f
			}
			// handle slice
		} else if x, ok := val.([]interface{}); ok {
			var f *graphql.Field
			f, err = buildSliceField(key, x)
			if err == nil {
				objFields[key] = f
			}
			// handle string
		} else if x, ok := val.(string); ok {
			if f, err := buildField(x); err == nil {
				objFields[key] = f
			} else {
				log.WithFields(log.Fields{"object": name, "field": key, "error": err}).Error("graphql: buildField ignoring field")
			}
			// handle other cases
		} else {
			err = fmt.Errorf("unhandled type: %T %v", val, val)
		}

		if err != nil {
			log.WithFields(log.Fields{"object": name, "field": key, "error": err}).Error("graphql: buildObject")
			// return nil, fmt.Errorf("object: %s: field: %s: error: %v", name, key, err)
		}
	}

	return graphql.NewObject(
		graphql.ObjectConfig{
			Name:   name,
			Fields: objFields,
		},
	), nil
}

type objectMap struct {
	objects     map[string]*graphql.Object
	edgeLabel   map[string]map[string]string
	edgeDstType map[string]map[string]string
}

// buildObjectMap scans the GripQL schema and turns all of the vertex types into different objects
func buildObjectMap(client gripql.Client, graph string, schema *gripql.Graph) (*objectMap, error) {
	objects := map[string]*graphql.Object{}
	edgeLabel := map[string]map[string]string{}
	edgeDstType := map[string]map[string]string{}

	for _, obj := range schema.Vertices {
		if obj.Label == "Vertex" {
			props := obj.GetDataMap()
			if props == nil {
				continue
			}
			props["id"] = "STRING"

			obj.Gid = lower_first_char(obj.Gid)
			gqlObj, err := buildObject(obj.Gid, props)
			if err != nil {
				return nil, err
			}
			if len(gqlObj.Fields()) > 0 {
				objects[obj.Gid] = gqlObj
			}
		}
		edgeLabel[obj.Gid] = map[string]string{}
		edgeDstType[obj.Gid] = map[string]string{}
	}

	fmt.Println("THE VALUE OF OBJECTS: ", objects)
	// Setup outgoing edge fields
	// Note: edge properties are not accessible in this model
	for i, obj := range schema.Edges {
		// The froms and tos are empty for some reason
		obj.From = lower_first_char(obj.From)
		if _, ok := objects[obj.From]; ok {
			obj.To = lower_first_char(obj.To)
			if _, ok := objects[obj.To]; ok {
				obj := obj // This makes an inner loop copy of the variable that is used by the Resolve function
				fname := obj.Label

				//ensure the fname is unique
				for j := range schema.Edges {
					if i != j {
						if schema.Edges[i].From == schema.Edges[j].From && schema.Edges[i].Label == schema.Edges[j].Label {
							fname = obj.Label + "_to_" + obj.To
						}
					}
				}
				//fmt.Println("OBJ.FROM: ", obj.From, "OBJ.TO: ", obj.To, "FNAME: ", fname, "OBJ.LABEL: ", obj.Label, "OBJ.DATA: ", obj.Data, "OBJ.GID: ", obj.Gid)
				edgeLabel[obj.From][fname] = obj.Label
				edgeDstType[obj.From][fname] = obj.To

				f := &graphql.Field{
					Name: fname,
					Type: graphql.NewList(objects[obj.To]),
					/*
						Resolve: func(p graphql.ResolveParams) (interface{}, error) {
							srcMap, ok := p.Source.(map[string]interface{})
							if !ok {
								return nil, fmt.Errorf("source conversion failed: %v", p.Source)
							}
							srcGid, ok := srcMap["id"].(string)
							if !ok {
								return nil, fmt.Errorf("source gid conversion failed: %+v", srcMap)
							}
							fmt.Printf("Field resolve: %s\n", srcGid)
							q := gripql.V(srcGid).HasLabel(obj.From).Out(obj.Label).HasLabel(obj.To)
							result, err := client.Traversal(&gripql.GraphQuery{Graph: graph, Query: q.Statements})
							if err != nil {
								return nil, err
							}
							out := []interface{}{}
							for r := range result {
								d := r.GetVertex().GetDataMap()
								d["id"] = r.GetVertex().Gid
								out = append(out, d)
							}
							return out, nil
						},
					*/
				}
				//fmt.Printf("building: %#v %s %s\n", f, obj.From, fname)
				objects[obj.From].AddFieldConfig(fname, f)
			}
		}
	}

	return &objectMap{objects: objects, edgeLabel: edgeLabel, edgeDstType: edgeDstType}, nil
}

func buildFieldConfigArgument(obj *graphql.Object) graphql.FieldConfigArgument {
	args := graphql.FieldConfigArgument{
		ARG_ID:     &graphql.ArgumentConfig{Type: graphql.String},
		ARG_IDS:    &graphql.ArgumentConfig{Type: graphql.NewList(graphql.String)},
		ARG_LIMIT:  &graphql.ArgumentConfig{Type: graphql.Int, DefaultValue: 100},
		ARG_OFFSET: &graphql.ArgumentConfig{Type: graphql.Int, DefaultValue: 0},
		ARG_FILTER: &graphql.ArgumentConfig{Type: JSONScalar},
	}
	if obj == nil {
		return args
	}
	for k, v := range obj.Fields() {
		switch v.Type {
		case graphql.String, graphql.Int, graphql.Float, graphql.Boolean:
			args[k] = &graphql.ArgumentConfig{Type: v.Type}
		default:
			continue
		}
	}
	return args
}

func lower_first_char(name string) string {
	temp := []rune(name)
	temp[0] = unicode.ToLower(temp[0])
	return string(temp)
}
func BubblesortByCount(list []any) []any {
	// Create a comparator that compares the "count" value of two maps.
	comparator := func(m1, m2 any) bool {
		count1 := m1.(map[string]any)["count"].(float64)
		count2 := m2.(map[string]any)["count"].(float64)
		return count2 > count1
	}

	n := len(list)
	// bubble sort the list by count
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if comparator(list[j], list[j+1]) {
				list[j], list[j+1] = list[j+1], list[j]
			}
		}
	}

	return list
}

func buildAggregationField(client gripql.Client, graph string, objects *objectMap) *graphql.Field {

	stringBucket := graphql.NewObject(graphql.ObjectConfig{
		Name: "BucketsForString",
		Fields: graphql.Fields{
			"key":   &graphql.Field{Name: "key", Type: graphql.String}, //EnumValueType
			"count": &graphql.Field{Name: "count", Type: graphql.Int},
		},
	})

	histogram := graphql.NewObject(graphql.ObjectConfig{
		Name: "Histogram",
		Fields: graphql.Fields{
			"histogram": &graphql.Field{
				Type: graphql.NewList(stringBucket),
			},
		},
	})

	queryFields := graphql.Fields{}

	for k, obj := range objects.objects {
		if len(obj.Fields()) > 0 {
			label := obj.Name()
			temp := []rune(label)
			temp[0] = unicode.ToUpper(temp[0])
			label = string(temp)

			aggFields := graphql.Fields{
				"_totalCount": &graphql.Field{Name: "_totalCount", Type: graphql.Int},
			}
			for k, v := range obj.Fields() {
				switch v.Type {
				case graphql.String:
					aggFields[k] =
						&graphql.Field{
							Name: k,
							Type: histogram,
						}
				}
			}

			ao := graphql.NewObject(graphql.ObjectConfig{
				Name:   k + "Aggregation",
				Fields: aggFields,
			})
			queryFields[k] = &graphql.Field{
				Name: k + "Aggregation",
				Type: ao,
				Args: graphql.FieldConfigArgument{
					"filter": &graphql.ArgumentConfig{Type: JSONScalar},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					var filter *FilterBuilder
					if filterArg, ok := p.Args[ARG_FILTER].(map[string]any); ok {
						fmt.Printf("Filter: %#v\n", filterArg)
						filter = NewFilterBuilder(filterArg)
					}
					q := gripql.V().HasLabel(label)
					if filter != nil {
						var err error
						q, err = filter.ExtendGrip(q)
						if err != nil {
							return nil, err
						}
					}

					aggs := []*gripql.Aggregate{
						{Name: "_totalCount", Aggregation: &gripql.Aggregate_Count{}},
					}

					counts := map[string][]any{}
					for _, i := range p.Info.FieldASTs {
						if i.SelectionSet != nil {
							for _, j := range i.SelectionSet.Selections {
								if k, ok := j.(*ast.Field); ok {
									if k.Name.Value != "_totalCount" {
										aggs = append(aggs, &gripql.Aggregate{
											Name: k.Name.Value,
											Aggregation: &gripql.Aggregate_Term{
												Term: &gripql.TermAggregation{
													Field: k.Name.Value,
												},
											},
										})
										counts[k.Name.Value] = []any{}
									}
								}
							}
						}
					}
					q = q.Aggregate(aggs)
					fmt.Println("QUERY: ", q.Statements)

					result, err := client.Traversal(&gripql.GraphQuery{Graph: graph, Query: q.Statements})
					if err != nil {
						return nil, err
					}
					out := map[string]any{}

					for i := range result {
						agg := i.GetAggregations()
						if agg.Name == "_totalCount" {
							out["_totalCount"] = int(agg.Value)
						} else {
							counts[agg.Name] = append(counts[agg.Name], map[string]any{
								"key":   agg.Key,
								"count": agg.Value,
							})
						}
					}
					for k, v := range counts {
						out[k] = map[string]any{"histogram": v}
					}

					keys := make([]string, 0, len(out))
					for key, value := range out {
						keys = append(keys, key)
						if key != "_totalCount" {
							if t, ok := value.(map[string]any)["histogram"].([]any); ok {
								t = BubblesortByCount(t)
							}
						}
					}

					return out, nil
				},
			}
			// add back in the name appendage after the &graphql.Field block so that it doesn't get picked up in the front end
			queryFields[k+"AggregationObject"] = queryFields[k]
		}
	}

	aggregationObject := graphql.NewObject(graphql.ObjectConfig{
		Name:   "AggregationObject",
		Fields: queryFields,
	})

	return &graphql.Field{
		Name: "_aggregation",
		Type: aggregationObject,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			// top level resolve doesn't do anything
			// but it needs to return an empty object so that the GraphQL
			// library will go to the child fields and call their resolvers
			return map[string]any{}, nil
		},
	}
}

type renderTree struct {
	fields    []string
	parent    map[string]string
	fieldName map[string]string
}

func (rt *renderTree) NewElement(cur string, fieldName string) string {
	rName := fmt.Sprintf("f%d", len(rt.fields))
	rt.fields = append(rt.fields, rName)
	rt.parent[rName] = cur
	rt.fieldName[rName] = fieldName
	return rName
}

func (om *objectMap) traversalBuild(query *gripql.Query, vertLabel string, field *ast.Field, curElement string, rt *renderTree, limit int, offset int) *gripql.Query {
	vertLabel = lower_first_char(vertLabel)
	moved := false
	for _, s := range field.SelectionSet.Selections {
		if k, ok := s.(*ast.Field); ok {
			fmt.Println("OM.EDGELABEL: ", om.edgeLabel)
			fmt.Println("OM.EDGEDSTYPE", om.edgeDstType)
			fmt.Println("vertLabel: ", vertLabel)
			fmt.Println("k.Name.Value: ", k.Name.Value)
			if _, ok := om.edgeLabel[vertLabel][k.Name.Value]; ok {
				if dstLabel, ok := om.edgeDstType[vertLabel][k.Name.Value]; ok {
					if moved {
						query = query.Select(curElement)
					}
					rName := rt.NewElement(curElement, k.Name.Value)
					query = query.OutNull(k.Name.Value).As(rName)

					// Additionally have to control the number of outputs on the results of each traversal
					// otherwise there are instances when you get all of the results for each traversal node
					query = query.Skip(uint32(offset)).Limit(uint32(limit))
					query = om.traversalBuild(query, dstLabel, k, rName, rt, limit, offset)
					moved = true
				}
			}
		}
	}
	return query
}

// buildQueryObject scans the built objects, which were derived from the list of vertex types
// found in the schema. It then build a query object that will take search parameters
// and create lists of objects of that type
func buildQueryObject(client gripql.Client, graph string, objects *objectMap) *graphql.Object {

	queryFields := graphql.Fields{}
	// For each of the objects that have been listed in the objectMap build a query entry point
	for objName, obj := range objects.objects {

		label := obj.Name()
		temp := []rune(label)
		temp[0] = unicode.ToUpper(temp[0])
		label = string(temp)
		f := &graphql.Field{
			Name: objName,
			Type: graphql.NewList(obj),
			Args: buildFieldConfigArgument(obj),
			Resolve: func(params graphql.ResolveParams) (interface{}, error) {

				q := gripql.V().HasLabel(label)
				if id, ok := params.Args[ARG_ID].(string); ok {
					fmt.Printf("Doing %s id=%s query", label, id)
					q = gripql.V(id).HasLabel(label)
				}
				if ids, ok := params.Args[ARG_IDS].([]string); ok {
					fmt.Printf("Doing %s ids=%s queries", label, ids)
					q = gripql.V(ids...).HasLabel(label)
				}
				var filter *FilterBuilder
				if filterArg, ok := params.Args[ARG_FILTER].(map[string]any); ok {
					fmt.Printf("Filter: %#v\n", filterArg)
					filter = NewFilterBuilder(filterArg)
				}
				for key, val := range params.Args {
					switch key {
					case ARG_ID, ARG_IDS, ARG_LIMIT, ARG_OFFSET, ARG_FILTER:
					default:
						q = q.Has(gripql.Eq(key, val))
					}
				}
				//if filter was passed, apply it
				fmt.Println("VALUE OF FILTER: ", params.Args, " VALUE OF QUERY: ", q.As("f0"))
				if filter != nil {
					var err error
					q, err = filter.ExtendGrip(q)
					if err != nil {
						return nil, err
					}
				}

				q = q.As("f0")
				limit := params.Args[ARG_LIMIT].(int)
				offset := params.Args[ARG_OFFSET].(int)
				q = q.Skip(uint32(offset)).Limit(uint32(limit))

				rt := &renderTree{
					fields:    []string{"f0"},
					parent:    map[string]string{},
					fieldName: map[string]string{},
				}
				fmt.Println("Q1: ", q)

				for _, f := range params.Info.FieldASTs {
					q = objects.traversalBuild(q, label, f, "f0", rt, limit, offset)
				}

				render := map[string]any{}
				for _, i := range rt.fields {
					render[i+"_gid"] = "$" + i + "._gid"
					render[i+"_data"] = "$" + i + "._data"
				}
				q = q.Render(render)

				//fmt.Println("Q2: ", q)
				result, err := client.Traversal(&gripql.GraphQuery{Graph: graph, Query: q.Statements})
				if err != nil {
					return nil, err
				}
				out := []interface{}{}
				for r := range result {
					values := r.GetRender().GetStructValue().AsMap()
					//fmt.Printf("render: %#v\n", values)
					data := map[string]map[string]any{}
					for _, r := range rt.fields {
						v := values[r+"_data"]
						if d, ok := v.(map[string]any); ok {
							d["id"] = values[r+"_gid"]
							if d["id"] != "" {
								data[r] = d
							}
						}
					}
					for _, r := range rt.fields {
						if parent, ok := rt.parent[r]; ok {
							fieldName := rt.fieldName[r]
							if data[r] != nil {
								data[parent][fieldName] = []any{data[r]}
							}
						}
					}
					//jtxt, _ := json.MarshalIndent(data["f0"], "", "  ")
					out = append(out, data["f0"])
					//fmt.Printf("ID query traversal: %s\n", r)
				}
				fmt.Println("Q2: ", q)
				return out, nil
			},
		}
		queryFields[objName] = f
	}

	queryFields["_aggregation"] = buildAggregationField(client, graph, objects)

	query := graphql.NewObject(
		graphql.ObjectConfig{
			Name:   "Query",
			Fields: queryFields,
		},
	)
	//fmt.Printf("Query fields: %#v\n", queryFields)
	return query
}
