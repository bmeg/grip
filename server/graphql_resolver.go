package server

import (
	"fmt"
	"strings"
	"time"

	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	log "github.com/sirupsen/logrus"
)

type gqlTranslator struct {
	query      *gripql.Query
	outKeys    []string
	outTmpl    map[string]map[string]interface{}
	edgeLabels []string
}

func (r *gqlTranslator) isEdgeLabel(label string) bool {
	for _, l := range r.edgeLabels {
		if label == l {
			return true
		}
	}
	return false
}

func (r *gqlTranslator) scanField(f *ast.Field, as string) error {
	if f.SelectionSet == nil {
		return nil
	}

	if as == "" {
		return fmt.Errorf("scanField: 'as' is an empty string")
	}

	parts := strings.Split(as, "__")
	outTmpl := r.outTmpl[parts[0]]
	for _, k := range parts[1:] {
		if val, ok := outTmpl[k]; ok {
			if mval, ok := val.(map[string]interface{}); ok {
				outTmpl = mval
			}
		}
	}

	// build up output rendering template
	// track which fields will be kept
	// track which fields are edges
	fields := []string{}
	edges := map[string]*ast.Field{}
	for _, s := range f.SelectionSet.Selections {
		if k, ok := s.(*ast.Field); ok {
			if r.isEdgeLabel(k.Name.Value) {
				outTmpl[k.Name.Value] = make(map[string]interface{})
				edges[k.Name.Value] = k
			} else {
				outTmpl[k.Name.Value] = "$" + as + "." + k.Name.Value
				fields = append(fields, k.Name.Value)
			}
		} else {
			return fmt.Errorf("unknown selection: %#v", s)
		}
	}

	// build up query; track mark names
	r.query = r.query.Fields(fields...)
	r.query = r.query.As(as)
	r.outKeys = append(r.outKeys, as)

	// TODO: figure out forked queries
	if len(edges) > 1 {
		return fmt.Errorf("branched queries not supported")
	}

	// continue traversal
	for eName, eField := range edges {
		r.query = r.query.Out(eName)
		return r.scanField(eField, as+"__"+eName)
	}

	return nil
}

func (r *gqlTranslator) translate(label string, params graphql.ResolveParams) (*gripql.Query, error) {
	r.outKeys = []string{}
	r.outTmpl = make(map[string]map[string]interface{})
	r.outTmpl[label] = make(map[string]interface{})
	r.query = gripql.NewQuery().V().HasLabel(label)

	for _, f := range params.Info.FieldASTs {
		err := r.scanField(f, label)
		if err != nil {
			return nil, fmt.Errorf("translating GraphQL query: %v", err)
		}
	}
	r.query = r.query.Select(r.outKeys...).Render(r.outTmpl)
	return r.query, nil
}

type GraphQLResolverConfig struct {
	DB         gdbi.GraphDB
	WorkDir    string
	Graph      string
	EdgeLabels []string
}

func ResolveGraphQL(conf GraphQLResolverConfig, label string, params graphql.ResolveParams) (interface{}, error) {
	start := time.Now()
	r := &gqlTranslator{edgeLabels: conf.EdgeLabels}
	query, err := r.translate(label, params)
	if err != nil {
		return nil, err
	}
	resultsChan, err := engine.Traversal(params.Context, conf.DB, conf.WorkDir, &gripql.GraphQuery{Graph: conf.Graph, Query: query.Statements})
	if err != nil {
		return nil, err
	}
	out := []interface{}{}
	for row := range resultsChan {
		d := row.GetVertex().GetDataMap()
		d["gid"] = row.GetVertex().Gid
		out = append(out, d)
	}
	log.WithFields(log.Fields{"query": query, "elapsed_time": time.Since(start)}).Debug("ResolveGraphQL")
	return out, nil
}
