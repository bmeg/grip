
package core

import (
  "fmt"
  "context"
  "math"
  "strings"
  "github.com/bmeg/grip/kvi"
  "github.com/bmeg/grip/kvindex"
  structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/influxdata/tdigest"
  "github.com/bmeg/grip/gripql"
  "github.com/bmeg/grip/jsonpath"

  "github.com/bmeg/grip/gdbi"
  "github.com/bmeg/grip/log"
  "github.com/bmeg/grip/protoutil"
)

type aggregate struct {
	aggregations []*gripql.Aggregate
}

func (agg *aggregate) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	aChans := make(map[string](chan []*gdbi.Traveler))

	go func() {
		for _, a := range agg.aggregations {
			aChans[a.Name] = make(chan []*gdbi.Traveler, 100)
			defer close(aChans[a.Name])
		}

		batchSize := 100
		i := 0
		batch := []*gdbi.Traveler{}
		for t := range in {
			if i == batchSize {
				for _, a := range agg.aggregations {
					aChans[a.Name] <- batch
				}
				i = 0
				batch = []*gdbi.Traveler{}
			}
			batch = append(batch, t)
			i++
		}
		for _, a := range agg.aggregations {
			aChans[a.Name] <- batch
		}
	}()

	go func() {
		defer close(out)
		for _, a := range agg.aggregations {
			a := a
			switch a.Aggregation.(type) {
			case *gripql.Aggregate_Term:
				tagg := a.GetTerm()
				size := tagg.Size
				kv := man.GetTempKV()
				idx := kvindex.NewIndex(kv)

				namespace := jsonpath.GetNamespace(tagg.Field)
				field := jsonpath.GetJSONPath(tagg.Field)
				field = strings.TrimPrefix(field, "$.")
				idx.AddField(field)

				tid := 0
				for batch := range aChans[a.Name] {
					err := kv.Update(func(tx kvi.KVTransaction) error {
						for _, t := range batch {
							doc := jsonpath.GetDoc(t, namespace)
							err := idx.AddDocTx(tx, fmt.Sprintf("%d", tid), doc)
							tid++
							if err != nil {
								return err
							}
						}
						return nil
					})
					if err != nil {
						log.Errorf("Error: aggregation index: %s", err)
					}
				}

				count := 0
				for tcount := range idx.FieldTermCounts(field) {
					var t *structpb.Value
					if tcount.String != "" {
						t = protoutil.WrapValue(tcount.String)
					} else {
						t = protoutil.WrapValue(tcount.Number)
					}
					if size <= 0 || count < int(size) {
						out <- &gdbi.Traveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: t, Value: float64(tcount.Count)}}
					}
					count++
				}

			case *gripql.Aggregate_Histogram:
				hagg := a.GetHistogram()
				interval := hagg.Interval
				kv := man.GetTempKV()
				idx := kvindex.NewIndex(kv)

				namespace := jsonpath.GetNamespace(hagg.Field)
				field := jsonpath.GetJSONPath(hagg.Field)
				field = strings.TrimPrefix(field, "$.")
				idx.AddField(field)

				tid := 0
				for batch := range aChans[a.Name] {
					err := kv.Update(func(tx kvi.KVTransaction) error {
						for _, t := range batch {
							doc := jsonpath.GetDoc(t, namespace)
							err := idx.AddDocTx(tx, fmt.Sprintf("%d", tid), doc)
							tid++
							if err != nil {
								return err
							}
						}
						return nil
					})
					if err != nil {
						log.Errorf("Error: aggregation index: %s", err)
					}
				}

				min := idx.FieldTermNumberMin(field)
				max := idx.FieldTermNumberMax(field)

				i := float64(interval)
				for bucket := math.Floor(min/i) * i; bucket <= max; bucket += i {
					var count uint64
					for tcount := range idx.FieldTermNumberRange(field, bucket, bucket+i) {
						count += tcount.Count
					}
					out <- &gdbi.Traveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: protoutil.WrapValue(bucket), Value: float64(count)}}
				}

			case *gripql.Aggregate_Percentile:

				pagg := a.GetPercentile()
				percents := pagg.Percents
				kv := man.GetTempKV()
				idx := kvindex.NewIndex(kv)

				namespace := jsonpath.GetNamespace(pagg.Field)
				field := jsonpath.GetJSONPath(pagg.Field)
				field = strings.TrimPrefix(field, "$.")
				idx.AddField(field)

				tid := 0
				for batch := range aChans[a.Name] {
					err := kv.Update(func(tx kvi.KVTransaction) error {
						for _, t := range batch {
							doc := jsonpath.GetDoc(t, namespace)
							err := idx.AddDocTx(tx, fmt.Sprintf("%d", tid), doc)
							tid++
							if err != nil {
								return err
							}
						}
						return nil
					})
					if err != nil {
						log.Errorf("Error: aggregation index: %s", err)
					}
				}

				td := tdigest.New()
				for val := range idx.FieldNumbers(field) {
					td.Add(val, 1)
				}

				for _, p := range percents {
					q := td.Quantile(p / 100)
					out <- &gdbi.Traveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: protoutil.WrapValue(p), Value: q}}
				}

			default:
				log.Errorf("Error: unknown aggregation type: %T", a.Aggregation)
				continue
			}
		}
	}()

	return ctx
}
