package sqlite

import (
	"fmt"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/lib/pq"
)

func (g *Graph) AddVertexIndex(vertexLabel string, field string) error {
	indexName := fmt.Sprintf("%s_%s_%s", g.graph, vertexLabel, field)
	query := fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS %s ON %s ((data->>'%s')) WHERE label = '%s'",
		pq.QuoteIdentifier(indexName),
		pq.QuoteIdentifier(g.v),
		field,
		vertexLabel,
	)
	_, err := g.db.Exec(query)
	if err != nil {
		log.Errorf("Error adding index %s: %v", indexName, err)
		return fmt.Errorf("failed to add index %s: %w", indexName, err)
	}
	log.Infof("Successfully added index %s for vertex label %s field %s", indexName, vertexLabel, field)
	return nil
}

// DeleteVertexIndex drops the specified index.
func (g *Graph) DeleteVertexIndex(vertexLabel string, field string) error {
	indexName := fmt.Sprintf("%s_%s_%s", g.graph, vertexLabel, field)
	query := fmt.Sprintf("DROP INDEX IF EXISTS %s", pq.QuoteIdentifier(indexName))
	_, err := g.db.Exec(query)
	if err != nil {
		log.Errorf("Error deleting index %s: %v", indexName, err)
		return fmt.Errorf("failed to delete index %s: %w", indexName, err)
	}
	log.Infof("Successfully deleted index %s", indexName)
	return nil
}

// GetVertexIndexList lists indices
func (g *Graph) GetVertexIndexList() <-chan *gripql.IndexID {
	o := make(chan *gripql.IndexID, 100)
	go func() {
		defer close(o)
		// Query indices only on the vertex table (g.v)
		query := `
            SELECT name
            FROM sqlite_master
            WHERE type = 'index' AND tbl_name = ?
              AND name NOT LIKE 'sqlite_autoindex_%'  -- Exclude auto-generated primary key indices
              AND name IN (
                  SELECT name
                  FROM pragma_index_list(?)
                  WHERE "unique" = 0  -- Exclude unique indices
              )
        `
		rows, err := g.db.Queryx(query, g.v, g.v)
		if err != nil {
			log.Errorf("Error getting index list: %v", err)
			return
		}
		defer rows.Close()

		prefix := fmt.Sprintf("%s_", g.graph)
		for rows.Next() {
			var indexName string
			if err := rows.Scan(&indexName); err != nil {
				log.Errorf("Error scanning index row: %v", err)
				continue
			}
			if !strings.HasPrefix(indexName, prefix) {
				log.Infof("Skipping index '%s': does not match graph '%s'", indexName, g.graph)
				continue
			}
			rest := strings.TrimPrefix(indexName, prefix)
			if strings.HasPrefix(rest, "vertices_") {
				continue
			} else {
				parts := strings.SplitN(rest, "_", 2)
				if len(parts) == 2 {
					o <- &gripql.IndexID{
						Graph: g.graph,
						Label: parts[0], // Vertex label, e.g., "Person"
						Field: parts[1], // Field, e.g., "age"
					}
				} else {
					log.Infof("Skipping index '%s': unrecognized format", indexName)
				}
			}
		}
		if err := rows.Err(); err != nil {
			log.Errorf("Error during index row iteration: %v", err)
		}
	}()
	return o
}
