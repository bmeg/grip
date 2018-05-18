package mongo

import (
	"context"
	"log"
	"strconv"
	"strings"

	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	"github.com/bmeg/arachne/protoutil"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"gopkg.in/mgo.v2/bson"
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

// Process runs the mongo aggregation pipeline
func (proc *Processor) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	log.Printf("Running Mongo Processor: %+v", proc.query)

	go func() {
		session := proc.db.ar.pool.Get()
		defer proc.db.ar.pool.Put(session)
		defer close(out)

		initCol := session.DB(proc.db.ar.database).C(proc.startCollection)
		for t := range in {
			iter := initCol.Pipe(proc.query).Iter()
			result := map[string]interface{}{}
			for iter.Next(&result) {
				log.Printf("Mongo Pipeline result: %+v", result)
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
								gid, ok := v.(string)
								if !ok {
									log.Printf("Failed to process selection data: %+v", v)
								}
								de := &gdbi.DataElement{}
								switch proc.markTypes[k] {
								case gdbi.VertexData:
									v := proc.db.GetVertex(gid, true)
									if v != nil {
										de = &gdbi.DataElement{
											ID:    v.Gid,
											Label: v.Label,
											Data:  v.GetDataMap(),
										}
									}
								case gdbi.EdgeData:
									e := proc.db.GetEdge(gid, true)
									if e != nil {
										de = &gdbi.DataElement{
											ID:    e.Gid,
											Label: e.Label,
											From:  e.From,
											To:    e.To,
											Data:  e.GetDataMap(),
										}
									}
								}
								selections[k] = de
							}
						}
					}
					out <- &gdbi.Traveler{Selections: selections}

				case gdbi.AggregationData:
					aggs := map[string]*aql.AggregationResult{}

					for k, v := range result {
						out := &aql.AggregationResult{
							Buckets: []*aql.AggregationResultBucket{},
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
								out.Buckets = append(out.Buckets, &aql.AggregationResultBucket{Key: term, Value: float64(count)})
							case float64:
								count := bucket["count"].(float64)
								out.Buckets = append(out.Buckets, &aql.AggregationResultBucket{Key: term, Value: count})
							default:
								log.Println("unexpected aggregation result type")
								continue
							}
						}
						aggs[k] = out
					}
					out <- &gdbi.Traveler{Aggregations: aggs}

				default:
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
			if err := iter.Err(); err != nil {
				log.Println("Mongo traversal error:", err)
				continue
			}
		}
	}()

	return ctx
}
