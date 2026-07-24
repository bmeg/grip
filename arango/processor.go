package arango

import (
	"context"
	"strings"

	"github.com/arangodb/go-driver/v2/arangodb"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/log"
)

func decodePathPayload(raw any) ([]gdbi.DataElementID, bool) {
	steps, ok := raw.([]any)
	if !ok {
		return nil, false
	}

	out := make([]gdbi.DataElementID, 0, len(steps))
	for _, step := range steps {
		entry, ok := step.(map[string]any)
		if !ok {
			continue
		}
		if vertex, ok := entry["vertex"].(string); ok {
			out = append(out, gdbi.DataElementID{Vertex: decodeDocumentKey(vertex)})
			continue
		}
		if edge, ok := entry["edge"].(string); ok {
			out = append(out, gdbi.DataElementID{Edge: decodeDocumentKey(edge)})
		}
	}

	return out, len(out) > 0
}

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

				currentDoc := result
				if v, ok := result[transpilerCurrentField].(map[string]any); ok {
					currentDoc = v
				}

				vertex := unpackVertex(currentDoc)
				emitTraveler := t

				if pathIDs, ok := decodePathPayload(result[transpilerPathField]); ok {
					if bt, ok := t.Copy().(*gdbi.BaseTraveler); ok {
						bt.Path = pathIDs
						bt.Current = &gdbi.DataElement{ID: vertex.ID, Label: vertex.Label, Loaded: true}
						emitTraveler = bt
					}
				}

				out <- emitTraveler.AddCurrent(vertex)
			}

			if err := cursor.Close(); err != nil {
				log.WithFields(log.Fields{"error": err}).Error("Arango cursor close failed")
			}
		}
		log.Debug("Arango Transpiler Processor Finished")
	}()

	return ctx
}
