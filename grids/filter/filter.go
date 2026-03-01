package filter

import (
	"strconv"
	"strings"
	"sync"

	bFilters "github.com/bmeg/benchtop/filters"
	"github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"github.com/bytedance/sonic/ast"
)

type GripQLFilter struct {
	Expression *gripql.HasExpression

	condCache sync.Map
	pathCache sync.Map
}

func (f *GripQLFilter) GetFilter() any {
	return f.Expression
}

func (f *GripQLFilter) IsNoOp() bool {
	return f.Expression == nil
}

func (f *GripQLFilter) Matches(row []byte, tableName string) bool {
	return f.matchesHasExpression(row, f.Expression, tableName)
}

// MatchesRawPayload evaluates the expression directly on raw JSON payload text
// plus explicit id/table context, avoiding payload re-wrapping allocations.
func (f *GripQLFilter) MatchesRawPayload(id string, payload string, tableName string) bool {
	return f.matchesHasExpressionRaw(payload, f.Expression, tableName, id)
}

func (f *GripQLFilter) RequiredFields() []string {
	return extractKeys(f.Expression)
}

type pathCacheEntry struct {
	directPath []any
	hasDirect  bool
	legacyPath []any
	hasLegacy  bool
}

func (f *GripQLFilter) getConditionFilter(cond *gripql.HasCondition) *bFilters.FieldFilter {
	if cond == nil {
		return nil
	}
	if v, ok := f.condCache.Load(cond); ok {
		if cf, ok := v.(*bFilters.FieldFilter); ok {
			return cf
		}
	}
	cf := &bFilters.FieldFilter{
		Operator: ToQueryCondition(cond.Condition),
		Field:    cond.Key,
		Value:    cond.Value.AsInterface(),
	}
	f.condCache.Store(cond, cf)
	return cf
}

func (f *GripQLFilter) getPathCacheEntry(condKey string) pathCacheEntry {
	if v, ok := f.pathCache.Load(condKey); ok {
		if e, ok := v.(pathCacheEntry); ok {
			return e
		}
	}
	e := pathCacheEntry{}
	if path, ok := parseDirectPath(condKey); ok {
		e.directPath = path
		e.hasDirect = true
	}
	if pathArr, err := table.ConvertJSONPathToArray(condKey); err == nil {
		e.legacyPath = pathArr
		e.hasLegacy = true
	}
	f.pathCache.Store(condKey, e)
	return e
}

func (f *GripQLFilter) lookupBytesCached(row []byte, condKey string) any {
	e := f.getPathCacheEntry(condKey)
	if e.hasDirect {
		node, err := sonic.Get(row, e.directPath...)
		if err == nil {
			v, ierr := node.Interface()
			if ierr == nil {
				return v
			}
		}
	}
	if e.hasLegacy {
		node, err := sonic.Get(row, e.legacyPath...)
		if err != nil {
			if err != ast.ErrNotExist {
				log.Debugf("Sonic fetch error for path %v: %v", e.legacyPath, err)
			}
			return nil
		}
		v, ierr := node.Interface()
		if ierr != nil {
			return nil
		}
		return v
	}
	return nil
}

func (f *GripQLFilter) lookupStringCached(row string, condKey string) any {
	e := f.getPathCacheEntry(condKey)
	if e.hasDirect {
		node, err := sonic.GetFromString(row, e.directPath...)
		if err == nil {
			v, ierr := node.Interface()
			if ierr == nil {
				return v
			}
		}
	}
	if e.hasLegacy {
		node, err := sonic.GetFromString(row, e.legacyPath...)
		if err != nil {
			if err != ast.ErrNotExist {
				log.Debugf("Sonic fetch error for path %v: %v", e.legacyPath, err)
			}
			return nil
		}
		v, ierr := node.Interface()
		if ierr != nil {
			return nil
		}
		return v
	}
	return nil
}

func extractKeys(expr *gripql.HasExpression) []string {
	keys := map[string]struct{}{}

	var recurse func(*gripql.HasExpression)
	recurse = func(e *gripql.HasExpression) {
		if e == nil {
			return
		}
		switch st := e.Expression.(type) {
		case *gripql.HasExpression_Condition:
			keys[st.Condition.GetKey()] = struct{}{}
		case *gripql.HasExpression_And:
			for _, subExpr := range st.And.GetExpressions() {
				recurse(subExpr)
			}
		case *gripql.HasExpression_Or:
			for _, subExpr := range st.Or.GetExpressions() {
				recurse(subExpr)
			}
		case *gripql.HasExpression_Not:
			recurse(st.Not.GetNot())
		}
	}

	recurse(expr)

	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	return out
}

func parseDirectPath(path string) ([]any, bool) {
	path = strings.TrimSpace(path)
	path = strings.TrimPrefix(path, "$")
	path = strings.TrimPrefix(path, ".")
	if path == "" {
		return nil, false
	}

	parts := []any{}
	var token strings.Builder
	flushToken := func() {
		if token.Len() > 0 {
			parts = append(parts, token.String())
			token.Reset()
		}
	}

	for i := 0; i < len(path); i++ {
		ch := path[i]
		switch ch {
		case '.':
			flushToken()
		case '[':
			flushToken()
			j := i + 1
			for j < len(path) && path[j] != ']' {
				j++
			}
			if j >= len(path) || j == i+1 {
				return nil, false
			}
			idx, err := strconv.Atoi(path[i+1 : j])
			if err != nil {
				return nil, false
			}
			parts = append(parts, idx)
			i = j
		default:
			token.WriteByte(ch)
		}
	}
	flushToken()

	if len(parts) == 0 {
		return nil, false
	}
	return parts, true
}

func sonicLookup(row []byte, condKey string) any {
	if path, ok := parseDirectPath(condKey); ok {
		node, err := sonic.Get(row, path...)
		if err == nil {
			v, ierr := node.Interface()
			if ierr == nil {
				return v
			}
		}
	}

	// Legacy packed-row fallback used by older json table code paths.
	pathArr, err := table.ConvertJSONPathToArray(condKey)
	if err != nil {
		return nil
	}
	node, err := sonic.Get(row, pathArr...)
	if err != nil {
		if err != ast.ErrNotExist {
			log.Debugf("Sonic fetch error for path %v: %v", pathArr, err)
		}
		return nil
	}
	v, ierr := node.Interface()
	if ierr != nil {
		return nil
	}
	return v
}

func sonicLookupString(row string, condKey string) any {
	if path, ok := parseDirectPath(condKey); ok {
		node, err := sonic.GetFromString(row, path...)
		if err == nil {
			v, ierr := node.Interface()
			if ierr == nil {
				return v
			}
		}
	}

	// Legacy packed-row fallback used by older json table code paths.
	pathArr, err := table.ConvertJSONPathToArray(condKey)
	if err != nil {
		return nil
	}
	node, err := sonic.GetFromString(row, pathArr...)
	if err != nil {
		if err != ast.ErrNotExist {
			log.Debugf("Sonic fetch error for path %v: %v", pathArr, err)
		}
		return nil
	}
	v, ierr := node.Interface()
	if ierr != nil {
		return nil
	}
	return v
}

func tableLabel(tableName string) string {
	if len(tableName) > 2 && (strings.HasPrefix(tableName, "v_") || strings.HasPrefix(tableName, "e_")) {
		return tableName[2:]
	}
	return tableName
}

func MatchesHasExpression(row []byte, stmt *gripql.HasExpression, tableName string) bool {
	if stmt == nil || stmt.Expression == nil {
		return true
	}

	switch stmt.Expression.(type) {
	case *gripql.HasExpression_Condition:
		cond := stmt.GetCondition()
		var lookupVal any

		switch cond.Key {
		case "_label":
			lookupVal = tableLabel(tableName)
		case "_id":
			node, err := sonic.Get(row, "_id")
			if err == nil {
				lookupVal, err = node.Interface()
				if err != nil {
					lookupVal = nil
				}
			} else {
				// Legacy packed-row fallback
				node, err = sonic.Get(row, []any{"1"}...)
				if err == nil {
					lookupVal, _ = node.Interface()
				}
			}
		default:
			lookupVal = sonicLookup(row, cond.Key)
		}

		return bFilters.ApplyFilterCondition(
			lookupVal,
			&bFilters.FieldFilter{
				Operator: ToQueryCondition(cond.Condition),
				Field:    cond.Key,
				Value:    cond.Value.AsInterface(),
			},
		)

	case *gripql.HasExpression_And:
		for _, e := range stmt.GetAnd().Expressions {
			if !MatchesHasExpression(row, e, tableName) {
				return false
			}
		}
		return true

	case *gripql.HasExpression_Or:
		for _, e := range stmt.GetOr().Expressions {
			if MatchesHasExpression(row, e, tableName) {
				return true
			}
		}
		return false

	case *gripql.HasExpression_Not:
		return !MatchesHasExpression(row, stmt.GetNot(), tableName)

	default:
		log.Errorf("unknown where expression type: %T", stmt.Expression)
		return false
	}
}

func MatchesHasExpressionRaw(payload string, stmt *gripql.HasExpression, tableName string, id string) bool {
	if stmt == nil || stmt.Expression == nil {
		return true
	}

	switch stmt.Expression.(type) {
	case *gripql.HasExpression_Condition:
		cond := stmt.GetCondition()
		var lookupVal any

		switch cond.Key {
		case "_label":
			lookupVal = tableLabel(tableName)
		case "_id":
			lookupVal = id
		default:
			lookupVal = sonicLookupString(payload, cond.Key)
		}

		return bFilters.ApplyFilterCondition(
			lookupVal,
			&bFilters.FieldFilter{
				Operator: ToQueryCondition(cond.Condition),
				Field:    cond.Key,
				Value:    cond.Value.AsInterface(),
			},
		)

	case *gripql.HasExpression_And:
		for _, e := range stmt.GetAnd().Expressions {
			if !MatchesHasExpressionRaw(payload, e, tableName, id) {
				return false
			}
		}
		return true

	case *gripql.HasExpression_Or:
		for _, e := range stmt.GetOr().Expressions {
			if MatchesHasExpressionRaw(payload, e, tableName, id) {
				return true
			}
		}
		return false

	case *gripql.HasExpression_Not:
		return !MatchesHasExpressionRaw(payload, stmt.GetNot(), tableName, id)

	default:
		log.Errorf("unknown where expression type: %T", stmt.Expression)
		return false
	}
}

func (f *GripQLFilter) matchesHasExpression(row []byte, stmt *gripql.HasExpression, tableName string) bool {
	if stmt == nil || stmt.Expression == nil {
		return true
	}

	switch stmt.Expression.(type) {
	case *gripql.HasExpression_Condition:
		cond := stmt.GetCondition()
		if cond == nil {
			return false
		}
		var lookupVal any
		switch cond.Key {
		case "_label":
			lookupVal = tableLabel(tableName)
		case "_id":
			node, err := sonic.Get(row, "_id")
			if err == nil {
				lookupVal, _ = node.Interface()
			} else {
				node, err = sonic.Get(row, []any{"1"}...)
				if err == nil {
					lookupVal, _ = node.Interface()
				}
			}
		default:
			lookupVal = f.lookupBytesCached(row, cond.Key)
		}
		cf := f.getConditionFilter(cond)
		if cf == nil {
			return false
		}
		return bFilters.ApplyFilterCondition(lookupVal, cf)

	case *gripql.HasExpression_And:
		for _, e := range stmt.GetAnd().Expressions {
			if !f.matchesHasExpression(row, e, tableName) {
				return false
			}
		}
		return true

	case *gripql.HasExpression_Or:
		for _, e := range stmt.GetOr().Expressions {
			if f.matchesHasExpression(row, e, tableName) {
				return true
			}
		}
		return false

	case *gripql.HasExpression_Not:
		return !f.matchesHasExpression(row, stmt.GetNot(), tableName)
	default:
		return false
	}
}

func (f *GripQLFilter) matchesHasExpressionRaw(payload string, stmt *gripql.HasExpression, tableName string, id string) bool {
	if stmt == nil || stmt.Expression == nil {
		return true
	}

	switch stmt.Expression.(type) {
	case *gripql.HasExpression_Condition:
		cond := stmt.GetCondition()
		if cond == nil {
			return false
		}
		var lookupVal any
		switch cond.Key {
		case "_label":
			lookupVal = tableLabel(tableName)
		case "_id":
			lookupVal = id
		default:
			lookupVal = f.lookupStringCached(payload, cond.Key)
		}
		cf := f.getConditionFilter(cond)
		if cf == nil {
			return false
		}
		return bFilters.ApplyFilterCondition(lookupVal, cf)

	case *gripql.HasExpression_And:
		for _, e := range stmt.GetAnd().Expressions {
			if !f.matchesHasExpressionRaw(payload, e, tableName, id) {
				return false
			}
		}
		return true

	case *gripql.HasExpression_Or:
		for _, e := range stmt.GetOr().Expressions {
			if f.matchesHasExpressionRaw(payload, e, tableName, id) {
				return true
			}
		}
		return false

	case *gripql.HasExpression_Not:
		return !f.matchesHasExpressionRaw(payload, stmt.GetNot(), tableName, id)
	default:
		return false
	}
}
