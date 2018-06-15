package elastic

import (
	"context"
	"fmt"
	"log"

	elastic "gopkg.in/olivere/elastic.v5"
)

func paginateQuery(ctx context.Context, q *elastic.SearchService, pageSize int) chan *elastic.SearchHit {
	o := make(chan *elastic.SearchHit, pageSize)
	go func() {
		defer close(o)
		done := false
		count := 0
		for {
			if done {
				return
			}
			res, err := q.From(count).Do(ctx)
			if err != nil {
				log.Println(fmt.Errorf("query failed: %v", err))
				return
			}
			if res.TotalHits() > 0 {
				for _, hit := range res.Hits.Hits {
					count++
					o <- hit
				}
			}
			if int64(count) == res.TotalHits() {
				done = true
			}
		}
	}()
	return o
}
