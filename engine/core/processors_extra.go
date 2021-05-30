package core

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/kvi"
	"github.com/bmeg/grip/kvindex"
	"github.com/influxdata/tdigest"
	"golang.org/x/sync/errgroup"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/log"
	"google.golang.org/protobuf/types/known/structpb"
)

type aggregateDisk struct {
	aggregations []*gripql.Aggregate
}

func (agg *aggregateDisk) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	aChans := make(map[string](chan []*gdbi.Traveler))
	g, ctx := errgroup.WithContext(ctx)

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

	for _, a := range agg.aggregations {
		a := a
		switch a.Aggregation.(type) {
		case *gripql.Aggregate_Term:
			g.Go(func() error {
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
						t, _ = structpb.NewValue(tcount.String)
					} else {
						t, _ = structpb.NewValue(tcount.Number)
					}
					if size <= 0 || count < int(size) {
						out <- &gdbi.Traveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: t, Value: float64(tcount.Count)}}
					}
					count++
				}
				return nil
			})

		case *gripql.Aggregate_Histogram:
			g.Go(func() error {
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
					sBucket, _ := structpb.NewValue(bucket)
					out <- &gdbi.Traveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: sBucket, Value: float64(count)}}
				}
				return nil
			})

		case *gripql.Aggregate_Percentile:
			g.Go(func() error {
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
					sp, _ := structpb.NewValue(p)
					out <- &gdbi.Traveler{Aggregation: &gdbi.Aggregate{Name: a.Name, Key: sp, Value: q}}
				}
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
