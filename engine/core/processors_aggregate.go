package core

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gdbi/tpath"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/influxdata/tdigest"
	"github.com/spf13/cast"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

type aggregate struct {
	aggregations []*gripql.Aggregate
}

func (agg *aggregate) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	aChans := make(map[string](chan gdbi.Traveler))
	g, ctx := errgroup.WithContext(ctx)

	// # of travelers to buffer for agg
	bufferSize := 1000
	for _, a := range agg.aggregations {
		aChans[a.Name] = make(chan gdbi.Traveler, bufferSize)
	}

	g.Go(func() error {
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			for _, a := range agg.aggregations {
				aChans[a.Name] <- t
			}
		}
		for _, a := range agg.aggregations {
			if aChans[a.Name] != nil {
				close(aChans[a.Name])
				//aChans[a.Name] = nil
			}
		}
		return nil
	})

	for _, a := range agg.aggregations {
		a := a
		switch a.Aggregation.(type) {
		case *gripql.Aggregate_Term:
			g.Go(func() error {
				// max # of terms to collect before failing
				// since the term can be a string this still isn't particularly safe
				// the terms could be arbitrarily large strings and storing this many could eat up
				// lots of memory.
				maxTerms := 100000

				tagg := a.GetTerm()
				size := tagg.Size

				// Collect error to return. Because we are reading a channel, it must be fully emptied
				// If we return error before fully emptying channel, upstream processes will lock
				var outErr error
				fieldTermCounts := map[any]int{}
				for t := range aChans[a.Name] {
					if len(fieldTermCounts) > maxTerms {
						outErr = fmt.Errorf("term aggreagtion: collected more unique terms (%v) than allowed (%v)", len(fieldTermCounts), maxTerms)
					} else {
						val := gdbi.TravelerPathLookup(t, tagg.Field)
						if val != nil {
							k := reflect.TypeOf(val).Kind()
							if k != reflect.Array && k != reflect.Slice && k != reflect.Map {
								fieldTermCounts[val]++

							}
						}
					}
				}

				count := 0
				for term, tcount := range fieldTermCounts {
					if size <= 0 || count < int(size) {
						//sTerm, _ := structpb.NewValue(term)
						//fmt.Printf("Term: %s %s %d\n", a.Name, sTerm, tcount)
						out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: term, Value: float64(tcount)}}
					}
				}
				return outErr
			})

		case *gripql.Aggregate_Histogram:

			g.Go(func() error {
				// max # of values to collect before failing
				maxValues := 10000000

				hagg := a.GetHistogram()
				i := float64(hagg.Interval)

				c := 0
				fieldValues := []float64{}

				// Collect error to return. Because we are reading a channel, it must be fully emptied
				// If we return error before fully emptying channel, upstream processes will lock
				var outErr error
				for t := range aChans[a.Name] {
					val := gdbi.TravelerPathLookup(t, hagg.Field)
					if val != nil {
						fval, err := cast.ToFloat64E(val)
						if err != nil {
							outErr = fmt.Errorf("histogram aggregation: can't convert %v to float64", val)
						}
						fieldValues = append(fieldValues, fval)
						if c > maxValues {
							outErr = fmt.Errorf("histogram aggreagtion: collected more values (%v) than allowed (%v)", c, maxValues)
						}
						c++
					}
				}
				if len(fieldValues) > 0 {
					sort.Float64s(fieldValues)
					min := fieldValues[0]
					max := fieldValues[len(fieldValues)-1]

					for bucket := math.Floor(min/i) * i; bucket <= max; bucket += i {
						var count float64
						for _, v := range fieldValues {
							if v >= bucket && v < (bucket+i) {
								count++
							}
						}
						//sBucket, _ := structpb.NewValue(bucket)
						out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: bucket, Value: float64(count)}}
					}
				}
				return outErr
			})

		case *gripql.Aggregate_Percentile:

			g.Go(func() error {
				pagg := a.GetPercentile()
				percents := pagg.Percents

				var outErr error
				td := tdigest.New()
				for t := range aChans[a.Name] {
					val := gdbi.TravelerPathLookup(t, pagg.Field)
					fval, err := cast.ToFloat64E(val)
					if err != nil {
						outErr = fmt.Errorf("percentile aggregation: can't convert %v to float64", val)
					}
					td.Add(fval, 1)
				}

				for _, p := range percents {
					q := td.Quantile(p / 100)
					//sp, _ := structpb.NewValue(p)
					out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: p, Value: q}}
				}

				return outErr
			})

		case *gripql.Aggregate_Field:
			g.Go(func() error {
				fa := a.GetField()
				fieldCounts := map[any]int{}
				for t := range aChans[a.Name] {
					val := gdbi.TravelerPathLookup(t, fa.Field)
					if m, ok := val.(map[string]any); ok {
						for k := range m {
							if !tpath.IsGraphField(k) {
								fieldCounts[k]++
							}
						}
					}
				}
				for term, tcount := range fieldCounts {
					out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: term, Value: float64(tcount)}}
				}
				return nil
			})

		case *gripql.Aggregate_Type:
			g.Go(func() error {
				fa := a.GetType()
				fieldTypes := map[string]int{}
				for t := range aChans[a.Name] {
					val := gdbi.TravelerPathLookup(t, fa.Field)
					tname := gripql.GetFieldType(val)
					fieldTypes[tname]++
				}
				for term, tcount := range fieldTypes {
					out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: term, Value: float64(tcount)}}
				}
				return nil
			})

		case *gripql.Aggregate_Count:
			g.Go(func() error {
				count := 0
				for range aChans[a.Name] {
					count++
				}
				out <- &gdbi.BaseTraveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: "count", Value: float64(count)}}
				return nil
			})

		default:
			log.Errorf("Error: unknown aggregation type: %T", a.Aggregation)
			continue
		}
	}

	go func() {
		if err := g.Wait(); err != nil {
			log.WithFields(log.Fields{"error": err}).Error("one or more aggregation failed")
		}
		close(out)
	}()

	return ctx
}
