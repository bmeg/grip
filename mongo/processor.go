package mongo

import (
	"context"
	"log"
	"strconv"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/protoutil"
	"github.com/globalsign/mgo/bson"
	structpb "github.com/golang/protobuf/ptypes/struct"
)

// aggType is a possible aggregation type
type aggType uint8

// aggTypes
const (
	unknownAgg aggType = iota
	termAgg
	histogramAgg
	percentileAgg
)

// Processor stores the information for a mongo aggregation pipeline
type Processor struct {
	db              *Graph
	startCollection string
	query           []bson.M
	dataType        gdbi.DataType
	markTypes       map[string]gdbi.DataType
	aggTypes        map[string]aggType
}

func getDataElement(result map[string]interface{}) *gdbi.DataElement {
	de := &gdbi.DataElement{}
	if x, ok := result["_id"]; ok {
		de.ID = x.(string)
	}
	if x, ok := result["label"]; ok {
		de.Label = x.(string)
	}
	if x, ok := result["data"]; ok {
		de.Data = x.(map[string]interface{})
	}
	if x, ok := result["to"]; ok {
		de.To = x.(string)
	}
	if x, ok := result["from"]; ok {
		de.From = x.(string)
	}
	return de
}

// Process runs the mongo aggregation pipeline
func (proc *Processor) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	// log.Printf("Running Mongo Processor: %+v", proc.query)

	go func() {
		session := proc.db.ar.session.Copy()
		defer session.Close()
		defer close(out)

		initCol := session.DB(proc.db.ar.database).C(proc.startCollection)
		for t := range in {
			nResults := 0
			iter := initCol.Pipe(proc.query).AllowDiskUse().Iter()
			defer iter.Close()
			result := map[string]interface{}{}
			for iter.Next(&result) {
				nResults++
				// log.Printf("Mongo Pipeline result: %+v", result)
				select {
				case <-ctx.Done():
					return
				default:
				}

				switch proc.dataType {
				case gdbi.CountData:
					eo := &gdbi.Traveler{}
					if x, ok := result["count"]; ok {
						eo.Count = uint32(x.(int))
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
					out <- &gdbi.Traveler{Selections: selections}

				case gdbi.AggregationData:
					aggs := map[string]*gripql.AggregationResult{}

					for k, v := range result {
						out := &gripql.AggregationResult{
							Buckets: []*gripql.AggregationResultBucket{},
						}

						buckets, ok := v.([]interface{})
						if !ok {
							log.Printf("Failed to convert Mongo aggregation result: %+v", v)
							continue
						}
						for _, bucket := range buckets {
							bucket, ok := bucket.(map[string]interface{})
							if !ok {
								log.Printf("Failed to convert Mongo aggregation result: %+v", bucket)
								continue
							}

							var term *structpb.Value
							switch proc.aggTypes[k] {
							case termAgg:
								term = protoutil.WrapValue(bucket["_id"])
							case histogramAgg:
								term = protoutil.WrapValue(bucket["_id"])
							case percentileAgg:
								bid := strings.Replace(bucket["_id"].(string), "_", ".", -1)
								f, err := strconv.ParseFloat(bid, 64)
								if err != nil {
									log.Printf("failed to parse percentile aggregation result key: %v", err)
									continue
								}
								term = protoutil.WrapValue(f)
							default:
								log.Println("unknown aggregation result type")
							}

							switch bucket["count"].(type) {
							case int:
								count := bucket["count"].(int)
								out.Buckets = append(out.Buckets, &gripql.AggregationResultBucket{Key: term, Value: float64(count)})
							case float64:
								count := bucket["count"].(float64)
								out.Buckets = append(out.Buckets, &gripql.AggregationResultBucket{Key: term, Value: count})
							default:
								log.Println("unexpected aggregation result type")
								continue
							}
						}
						aggs[k] = out
					}
					out <- &gdbi.Traveler{Aggregations: aggs}

				default:
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
					if x, ok := result["_id"]; ok {
						de.ID = x.(string)
					}
					if x, ok := result["label"]; ok {
						de.Label = x.(string)
					}
					if x, ok := result["data"]; ok {
						de.Data = x.(map[string]interface{})
					}
					if x, ok := result["to"]; ok {
						de.To = x.(string)
					}
					if x, ok := result["from"]; ok {
						de.From = x.(string)
					}
					out <- t.AddCurrent(de)
				}
			}
			if err := iter.Close(); err != nil {
				log.Println("Mongo traversal error:", err)
				continue
			}
			if nResults == 0 && proc.dataType == gdbi.CountData {
				out <- &gdbi.Traveler{Count: 0}
			}
		}
	}()
	return ctx
}
