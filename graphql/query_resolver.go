package graphql

import (
	"fmt"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	log "github.com/sirupsen/logrus"
)

func scanField(q *gripql.Query, f *ast.Field, as string) (*gripql.Query, error) {
	log.Infof("%+v", q)
	if f.SelectionSet != nil {
		fields := []string{}
		edges := map[string]*ast.Field{}
		for _, s := range f.SelectionSet.Selections {
			if k, ok := s.(*ast.Field); ok {
				if strings.HasPrefix(k.Name.Value, "edge_") {
					edges[k.Name.Value] = k
				} else {
					fields = append(fields, k.Name.Value)
				}
			} else {
				return nil, fmt.Errorf("unknown selection: %#v", s)
			}
		}
		q = q.Fields(fields...)
		if as != "" {
			q = q.As(as)
		}
		if len(edges) > 1 {
			return nil, fmt.Errorf("branched queries not supported")
		}
		for eName, eField := range edges {
			return scanField(q.Out(eName), eField, eName)
		}
	}
	return q, nil
}

func resolveGraphql(label string, p graphql.ResolveParams) (*gripql.Query, error) {
	var q *gripql.Query
	var err error

	q = gripql.NewQuery().V().HasLabel(label)

	for _, f := range p.Info.FieldASTs {
		q, err = scanField(q, f, label)
		if err != nil {
			return nil, err
		}
	}

	log.Infof("Query: %+v", q)
	return nil, fmt.Errorf("not implemented")
}
