package mongo

import (
	//"fmt"
	"context"
	"strconv"
	"strings"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Processor stores the information for a mongo aggregation pipeline
type Processor struct {
	db              *Graph
	startCollection string
	query           mongo.Pipeline
	dataType        gdbi.DataType
	markTypes       map[string]gdbi.DataType
	aggTypes        map[string]*gripql.Aggregate
}

func getDataElement(result map[string]interface{}) *gdbi.DataElement {
	de := &gdbi.DataElement{}
	if x, ok := result[FIELD_ID]; ok {
		de.ID = x.(string)
	}
	if x, ok := result[FIELD_LABEL]; ok {
		de.Label = x.(string)
	}
	de.Data = map[string]any{}
	for k, v := range removePrimatives(result).(map[string]any) {
		if !IsNodeField(k) {
			de.Data[k] = v
		}
	}
	de.Loaded = true
	if x, ok := result[FIELD_TO]; ok {
		de.To = x.(string)
	}
	if x, ok := result[FIELD_FROM]; ok {
		de.From = x.(string)
	}
	return de
}

// Process runs the mongo aggregation pipeline
func (proc *Processor) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	plog := log.WithFields(log.Fields{"query_id": util.UUID(), "query": proc.query, "query_collection": proc.startCollection})
	plog.Debug("Running Mongo Processor")

	go func() {
		defer close(out)

		initCol := proc.db.ar.client.Database(proc.db.ar.database).Collection(proc.startCollection)
		for t := range in {
			nResults := 0
			//plog.Infof("Running: %#v", proc.query)
			trueVal := true
			cursor, err := initCol.Aggregate(ctx, proc.query, &options.AggregateOptions{AllowDiskUse: &trueVal})
			if err != nil {
				plog.Errorf("Query Error (%s) : %s", proc.query, err)
				continue
			}
			//defer cursor.Close(context.TODO())
			result := map[string]interface{}{}
			for cursor.Next(ctx) {
				nResults++
				select {
				case <-ctx.Done():
					return
				default:
				}
				if nil != cursor.Decode(&result) {
					plog.Errorf("Result Error : %s", err)
					continue
				}
				//fmt.Printf("Data: %s\n", result)
				switch proc.dataType {
				case gdbi.CountData:
					eo := &gdbi.BaseTraveler{}
					if x, ok := result["count"]; ok {
						eo.Count = uint32(x.(int32))
					}
					out <- eo

				case gdbi.SelectionData:
					selections := map[string]*gdbi.DataElement{}
					if marks, ok := result["marks"]; ok {
						if marks, ok := marks.(map[string]interface{}); ok {
							for k, v := range marks {
								if v, ok := v.(map[string]interface{}); ok {
									de := getDataElement(v)
									selections[k] = de
								}
							}
						}
					}
					out <- &gdbi.BaseTraveler{Selections: selections}

				case gdbi.AggregationData:

					for k, v := range result {
						buckets, ok := v.(bson.A)
						if !ok {
							plog.Errorf("Failed to convert Mongo aggregation result (%s): %+v", k, v)
							continue
						}

						var lastBucket float64
						for i, bucket := range buckets {
							bucket, ok := bucket.(map[string]interface{})
							if !ok {
								plog.Errorf("Failed to convert Mongo aggregation result bucket: %+v", bucket)
								continue
							}

							var term interface{}
							switch proc.aggTypes[k].GetAggregation().(type) {
							case *gripql.Aggregate_Term:
								term = bucket["_id"]
							case *gripql.Aggregate_Histogram:
								term = bucket["_id"]
								curPos := bucket["_id"].(float64)
								stepSize := float64(proc.aggTypes[k].GetHistogram().Interval)
								if i != 0 {
									for nv := lastBucket + stepSize; nv < curPos; nv += stepSize {
										out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: k, Key: nv, Value: float64(0.0)}}
									}
								}
								lastBucket = curPos

							case *gripql.Aggregate_Percentile:
								bid := strings.Replace(bucket["_id"].(string), "_", ".", -1)
								f, err := strconv.ParseFloat(bid, 64)
								if err != nil {
									plog.Errorf("failed to parse percentile aggregation result key: %v", err)
									continue
								}
								term = f
							case *gripql.Aggregate_Field:
								term = bucket["_id"]
							case *gripql.Aggregate_Count:
								term = bucket["_id"]
							case *gripql.Aggregate_Type:
								switch bucket["_id"] {
								case "double":
									term = "NUMERIC"
								case "null":
									term = "UNKNOWN"
								case "string":
									term = "STRING"
								}
							default:
								plog.Errorf("unknown aggregation result type")
							}
							//fmt.Printf("term: %s %s", term, count)
							switch bucket["count"].(type) {
							case int:
								count := bucket["count"].(int)
								out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: k, Key: term, Value: float64(count)}}
							case int32:
								count := bucket["count"].(int32)
								out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: k, Key: term, Value: float64(count)}}
							case float64:
								count := bucket["count"].(float64)
								out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: k, Key: term, Value: float64(count)}}
							default:
								plog.Errorf("unexpected aggregation result type: %T", bucket["count"])
								continue
							}
						}
					}

				default:
					//Reconstruct the traveler
					//Extract the path
					if path, ok := result["path"]; ok {
						if pathA, ok := path.(bson.A); ok {
							o := make([]gdbi.DataElementID, len(pathA))
							for i := range pathA {
								if elem, ok := pathA[i].(map[string]interface{}); ok {
									if v, ok := elem["vertex"]; ok {
										o[i] = gdbi.DataElementID{Vertex: removePrimatives(v).(string)}
									} else if v, ok := elem["edge"]; ok {
										o[i] = gdbi.DataElementID{Edge: removePrimatives(v).(string)}
									}
								}
							}
							//t.Path = o
							t = &gdbi.BaseTraveler{Path: o}
						}
					}
					//Extract marks
					if marks, ok := result["marks"]; ok {
						if marks, ok := marks.(map[string]interface{}); ok {
							for k, v := range marks {
								if v, ok := v.(map[string]interface{}); ok {
									de := getDataElement(v)
									t = t.AddMark(k, de)
								}
							}
						}
					}

					de := &gdbi.DataElement{}
					data := removePrimatives(result["data"]).(map[string]any)
					if x, ok := data[FIELD_ID]; ok {
						de.ID = removePrimatives(x).(string)
					}
					if x, ok := data[FIELD_LABEL]; ok {
						de.Label = x.(string)
					}
					//if x, ok := result["data"]; ok {
					de.Data = RemoveKeyFields(data) //removePrimatives(x).(map[string]interface{})
					de.Loaded = true
					//}
					if x, ok := data[FIELD_TO]; ok {
						de.To = x.(string)
					}
					if x, ok := data[FIELD_FROM]; ok {
						de.From = x.(string)
					}
					out <- t.AddCurrent(de)
				}
			}
			if err := cursor.Close(context.TODO()); err != nil {
				plog.WithFields(log.Fields{"error": err}).Error("MongoDb: iterating results")
				continue
			}
			if nResults == 0 && proc.dataType == gdbi.CountData {
				out <- &gdbi.BaseTraveler{Count: 0}
			}
		}
		plog.Debug("Mongo Processor Finished")
	}()
	return ctx
}
