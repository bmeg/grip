package arango

import (
	"context"
	"strings"

	"github.com/arangodb/go-driver/v2/arangodb"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/log"
)

// Processor executes a transpiled Arango query and emits travelers.
type Processor struct {
	db  *Graph
	ast *ASTBase
}

// Process runs the transpiled AQL query.
func (proc *Processor) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	_ = man

	query := strings.TrimSpace(proc.ast.String())
	query = strings.ReplaceAll(query, " IN Vertices", " IN @@v")
	bindVars := map[string]any{"@v": proc.db.vertexCollection.Name()}

	log.Debug("Running Arango Transpiler Processor")

	go func() {
		defer close(out)

		for t := range in {
			cursor, err := proc.db.ar.db.Query(ctx, query, &arangodb.QueryOptions{BindVars: bindVars})
			if err != nil {
				log.WithFields(log.Fields{"error": err, "query": query}).Error("Arango query failed")
				continue
			}

			for cursor.HasMore() {
				select {
				case <-ctx.Done():
					_ = cursor.Close()
					return
				default:
				}

				result := map[string]any{}
				if _, err := cursor.ReadDocument(ctx, &result); err != nil {
					log.WithFields(log.Fields{"error": err}).Error("Arango result decode failed")
					break
				}

				vertex := unpackVertex(result)
				out <- t.AddCurrent(vertex)
			}

			if err := cursor.Close(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("Arango cursor close failed")
			}
		}
		log.Debug("Arango Transpiler Processor Finished")
	}()

	return ctx
}
