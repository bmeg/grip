package filter

import (
	"strconv"
	"strings"
	"sync"

	bFilters "github.com/bmeg/benchtop/filters"
	"github.com/bmeg/benchtop/jsontable/table"
	"github.com/bmeg/benchtop/query"
	"github.com/bmeg/grip/gdbi/tpath"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bytedance/sonic"
	"github.com/bytedance/sonic/ast"
)

type GripQLFilter struct {
	Expression *gripql.HasExpression

	condCache      sync.Map
	condEvalCache  sync.Map
	pathCache      sync.Map
	stringSetCache sync.Map
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
	directPath       []any
	packedDirectPath []any
	hasDirect        bool
	hasWildcard      bool
	legacyPath       []any
	hasLegacy        bool
}

type stringSetCacheEntry struct {
	set map[string]struct{}
	ok  bool
}

type conditionEvalCacheEntry struct {
	op query.Condition

	raw any

	hasString bool
	stringVal string

	hasNumeric bool
	numericVal float64

	hasBounds bool
	lower     float64
	upper     float64

	hasStringSet bool
	stringSet    map[string]struct{}

	hasNumericSet bool
	numericSet    map[float64]struct{}
}

func conditionValue(cond *gripql.HasCondition) any {
	if cond == nil || cond.Value == nil {
		return nil
	}
	return cond.Value.AsInterface()
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
		Value:    conditionValue(cond),
	}
	f.condCache.Store(cond, cf)
	return cf
}

func asAnySlice(v any) ([]any, bool) {
	switch vals := v.(type) {
	case []any:
		return vals, true
	case []string:
		out := make([]any, len(vals))
		for i := range vals {
			out[i] = vals[i]
		}
		return out, true
	default:
		return nil, false
	}
}

func looksNumericString(s string) bool {
	if s == "" {
		return false
	}
	hasDigit := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch >= '0' && ch <= '9' {
			hasDigit = true
			continue
		}
		switch ch {
		case '+', '-', '.', 'e', 'E':
		default:
			return false
		}
	}
	return hasDigit
}

func toFloat64Fast(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case string:
		if !looksNumericString(n) {
			return 0, false
		}
		out, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0, false
		}
		return out, true
	default:
		return 0, false
	}
}

func (f *GripQLFilter) getConditionEval(cond *gripql.HasCondition) conditionEvalCacheEntry {
	if cond == nil {
		return conditionEvalCacheEntry{}
	}
	if v, ok := f.condEvalCache.Load(cond); ok {
		if e, ok := v.(conditionEvalCacheEntry); ok {
			return e
		}
	}

	op := ToQueryCondition(cond.Condition)
	val := conditionValue(cond)
	e := conditionEvalCacheEntry{
		op:  op,
		raw: val,
	}

	if s, ok := val.(string); ok {
		e.hasString = true
		e.stringVal = s
	}
	if n, ok := toFloat64Fast(val); ok {
		e.hasNumeric = true
		e.numericVal = n
	}

	switch op {
	case query.INSIDE, query.OUTSIDE, query.BETWEEN:
		if vals, ok := asAnySlice(val); ok && len(vals) == 2 {
			lo, okLo := toFloat64Fast(vals[0])
			hi, okHi := toFloat64Fast(vals[1])
			if okLo && okHi {
				e.hasBounds = true
				e.lower = lo
				e.upper = hi
			}
		}
	case query.WITHIN, query.WITHOUT:
		if vals, ok := asAnySlice(val); ok {
			sset := make(map[string]struct{}, len(vals))
			nset := make(map[float64]struct{}, len(vals))
			allStrings := true
			allNumbers := true
			for _, v := range vals {
				if s, ok := v.(string); ok {
					sset[s] = struct{}{}
				} else {
					allStrings = false
				}
				if n, ok := toFloat64Fast(v); ok {
					nset[n] = struct{}{}
				} else {
					allNumbers = false
				}
			}
			if allStrings {
				e.hasStringSet = true
				e.stringSet = sset
			}
			if allNumbers {
				e.hasNumericSet = true
				e.numericSet = nset
			}
		}
	}

	f.condEvalCache.Store(cond, e)
	return e
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
		e.packedDirectPath = make([]any, 1, 1+len(path))
		e.packedDirectPath[0] = "0"
		e.packedDirectPath = append(e.packedDirectPath, path...)
		e.hasDirect = true
		e.hasWildcard = pathHasWildcard(path)
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
		if e.hasWildcard {
			return lookupBytesWildcard(row, e.directPath)
		}
		// Try packed format (field "0")
		node, err := sonic.Get(row, e.packedDirectPath...)
		if err == nil {
			v, _ := node.Interface()
			return v
		}
		// Try unpacked format (top level)
		node, err = sonic.Get(row, e.directPath...)
		if err == nil {
			v, _ := node.Interface()
			return v
		}
		// Field path is valid but not found: return sentinel
		return bFilters.FieldAbsent
	}
	if e.hasLegacy {
		node, err := sonic.Get(row, e.legacyPath...)
		if err != nil {
			if err != ast.ErrNotExist {
				log.Debugf("Sonic fetch error for path %v: %v", e.legacyPath, err)
			}
			return bFilters.FieldAbsent
		}
		v, ierr := node.Interface()
		if ierr != nil {
			return bFilters.FieldAbsent
		}
		return v
	}
	return bFilters.FieldAbsent
}

func (f *GripQLFilter) lookupStringCached(row string, condKey string) any {
	e := f.getPathCacheEntry(condKey)
	if e.hasDirect {
		if e.hasWildcard {
			return lookupStringWildcard(row, e.directPath)
		}
		// Try packed format (field "0")
		node, err := sonic.GetFromString(row, e.packedDirectPath...)
		if err == nil {
			v, _ := node.Interface()
			return v
		}
		// Try unpacked format (top level)
		node, err = sonic.GetFromString(row, e.directPath...)
		if err == nil {
			v, _ := node.Interface()
			return v
		}
		// Field path is valid but not found: return sentinel
		return bFilters.FieldAbsent
	}
	if e.hasLegacy {
		node, err := sonic.GetFromString(row, e.legacyPath...)
		if err != nil {
			if err != ast.ErrNotExist {
				log.Debugf("Sonic fetch error for path %v: %v", e.legacyPath, err)
			}
			return bFilters.FieldAbsent
		}
		v, ierr := node.Interface()
		if ierr != nil {
			return bFilters.FieldAbsent
		}
		return v
	}
	return bFilters.FieldAbsent
}

func (f *GripQLFilter) getConditionStringSet(cond *gripql.HasCondition) (map[string]struct{}, bool) {
	if cond == nil {
		return nil, false
	}
	if cached, ok := f.stringSetCache.Load(cond); ok {
		if entry, ok := cached.(stringSetCacheEntry); ok {
			return entry.set, entry.ok
		}
	}
	cf := f.getConditionFilter(cond)
	if cf == nil {
		f.stringSetCache.Store(cond, stringSetCacheEntry{})
		return nil, false
	}
	entry := stringSetCacheEntry{set: map[string]struct{}{}, ok: true}
	switch vals := cf.Value.(type) {
	case []string:
		for _, s := range vals {
			entry.set[s] = struct{}{}
		}
	case []any:
		for _, v := range vals {
			s, ok := v.(string)
			if !ok {
				entry.ok = false
				entry.set = nil
				break
			}
			entry.set[s] = struct{}{}
		}
	default:
		entry.ok = false
		entry.set = nil
	}
	f.stringSetCache.Store(cond, entry)
	return entry.set, entry.ok
}

func (f *GripQLFilter) applyCondition(lookupVal any, cond *gripql.HasCondition) bool {
	if cond == nil {
		return false
	}
	if _, absent := lookupVal.(bFilters.FieldAbsentType); absent {
		return false
	}

	ce := f.getConditionEval(cond)
	op := ce.op
	condVal := ce.raw

	if vals, ok := lookupVal.([]any); ok && op != query.CONTAINS {
		if len(vals) == 0 {
			return false
		}
		switch op {
		case query.NEQ, query.WITHOUT:
			for _, item := range vals {
				if !f.applyCondition(item, cond) {
					return false
				}
			}
			return true
		default:
			for _, item := range vals {
				if f.applyCondition(item, cond) {
					return true
				}
			}
			return false
		}
	}

	if lookupS, ok := lookupVal.(string); ok {
		switch op {
		case query.EQ:
			if ce.hasString {
				return lookupS == ce.stringVal
			}
		case query.NEQ:
			if condVal == nil {
				return true
			}
			if ce.hasString {
				return lookupS != ce.stringVal
			}
		case query.WITHIN:
			if ce.hasStringSet {
				set := ce.stringSet
				_, found := set[lookupS]
				return found
			}
		case query.WITHOUT:
			if ce.hasStringSet {
				set := ce.stringSet
				_, found := set[lookupS]
				return !found
			}
		}
	}

	switch op {
	case query.EQ:
		if condVal == nil {
			return lookupVal == nil
		}
		if ce.hasNumeric {
			if n, ok := toFloat64Fast(lookupVal); ok {
				return n == ce.numericVal
			}
		}
	case query.NEQ:
		if condVal == nil {
			return lookupVal != nil
		}
		if ce.hasNumeric {
			if n, ok := toFloat64Fast(lookupVal); ok {
				return n != ce.numericVal
			}
		}
		if ce.hasString {
			if _, ok := lookupVal.(string); !ok {
				return true
			}
		}
	case query.GT:
		if ce.hasNumeric {
			if n, ok := toFloat64Fast(lookupVal); ok {
				return n > ce.numericVal
			}
			return false
		}
	case query.GTE:
		if ce.hasNumeric {
			if n, ok := toFloat64Fast(lookupVal); ok {
				return n >= ce.numericVal
			}
			return false
		}
	case query.LT:
		if ce.hasNumeric {
			if n, ok := toFloat64Fast(lookupVal); ok {
				return n < ce.numericVal
			}
			return false
		}
	case query.LTE:
		if ce.hasNumeric {
			if n, ok := toFloat64Fast(lookupVal); ok {
				return n <= ce.numericVal
			}
			return false
		}
	case query.INSIDE:
		if ce.hasBounds {
			if n, ok := toFloat64Fast(lookupVal); ok {
				return n > ce.lower && n < ce.upper
			}
			return false
		}
	case query.BETWEEN:
		if ce.hasBounds {
			if n, ok := toFloat64Fast(lookupVal); ok {
				return n >= ce.lower && n <= ce.upper
			}
			return false
		}
	case query.OUTSIDE:
		if ce.hasBounds {
			if n, ok := toFloat64Fast(lookupVal); ok {
				return n < ce.lower || n > ce.upper
			}
			return false
		}
	case query.WITHIN:
		if ce.hasNumericSet {
			if n, ok := toFloat64Fast(lookupVal); ok {
				_, found := ce.numericSet[n]
				return found
			}
		}
	case query.WITHOUT:
		if ce.hasNumericSet {
			if n, ok := toFloat64Fast(lookupVal); ok {
				_, found := ce.numericSet[n]
				return !found
			}
		}
	case query.CONTAINS:
		if ce.hasString {
			switch vals := lookupVal.(type) {
			case []string:
				for _, s := range vals {
					if s == ce.stringVal {
						return true
					}
				}
				return false
			case []any:
				for _, v := range vals {
					if s, ok := v.(string); ok && s == ce.stringVal {
						return true
					}
				}
				return false
			}
		}
		if ce.hasNumeric {
			switch vals := lookupVal.(type) {
			case []any:
				for _, v := range vals {
					if n, ok := toFloat64Fast(v); ok && n == ce.numericVal {
						return true
					}
				}
				return false
			}
		}
	}

	cf := f.getConditionFilter(cond)
	if cf == nil {
		return false
	}
	return bFilters.ApplyFilterCondition(lookupVal, cf)
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
	return tpath.ParseDirectPath(path)
}

func pathHasWildcard(path []any) bool {
	for _, seg := range path {
		if _, ok := seg.(tpath.WildcardToken); ok {
			return true
		}
	}
	return false
}

func lookupDirectPathValue(root any, path []any) (any, bool) {
	candidates := []any{root}
	for _, seg := range path {
		next := make([]any, 0, len(candidates))
		switch s := seg.(type) {
		case tpath.WildcardToken:
			for _, cand := range candidates {
				next = appendWildcardCandidates(next, cand)
			}
		case string:
			for _, cand := range candidates {
				next = appendStringCandidates(next, cand, s)
			}
		case int:
			for _, cand := range candidates {
				next = appendIndexCandidates(next, cand, s)
			}
		default:
			return nil, false
		}
		if len(next) == 0 {
			return nil, false
		}
		candidates = next
	}
	if len(candidates) == 1 {
		return candidates[0], true
	}
	return candidates, true
}

func appendWildcardCandidates(out []any, val any) []any {
	switch cv := val.(type) {
	case []any:
		out = append(out, cv...)
	case []map[string]any:
		for _, item := range cv {
			out = append(out, item)
		}
	case map[string]any:
		for _, item := range cv {
			out = append(out, item)
		}
	}
	return out
}

func appendStringCandidates(out []any, val any, key string) []any {
	switch cv := val.(type) {
	case map[string]any:
		if v, ok := cv[key]; ok {
			out = append(out, v)
		}
	case []any:
		for _, item := range cv {
			out = appendStringCandidates(out, item, key)
		}
	case []map[string]any:
		for _, m := range cv {
			if v, ok := m[key]; ok {
				out = append(out, v)
			}
		}
	}
	return out
}

func appendIndexCandidates(out []any, val any, idx int) []any {
	switch arr := val.(type) {
	case []any:
		if idx >= 0 && idx < len(arr) {
			out = append(out, arr[idx])
		}
	case []map[string]any:
		if idx >= 0 && idx < len(arr) {
			out = append(out, arr[idx])
		}
	}
	return out
}

func lookupBytesWildcard(row []byte, path []any) any {
	var doc any
	if err := sonic.Unmarshal(row, &doc); err != nil {
		return bFilters.FieldAbsent
	}
	if root, ok := doc.(map[string]any); ok {
		if packed, found := root["0"]; found {
			if v, ok := lookupDirectPathValue(packed, path); ok {
				return v
			}
		}
	}
	if v, ok := lookupDirectPathValue(doc, path); ok {
		return v
	}
	return bFilters.FieldAbsent
}

func lookupStringWildcard(row string, path []any) any {
	var doc any
	if err := sonic.UnmarshalString(row, &doc); err != nil {
		return bFilters.FieldAbsent
	}
	if root, ok := doc.(map[string]any); ok {
		if packed, found := root["0"]; found {
			if v, ok := lookupDirectPathValue(packed, path); ok {
				return v
			}
		}
	}
	if v, ok := lookupDirectPathValue(doc, path); ok {
		return v
	}
	return bFilters.FieldAbsent
}

func sonicLookup(row []byte, condKey string) any {
	if path, ok := parseDirectPath(condKey); ok {
		if pathHasWildcard(path) {
			return lookupBytesWildcard(row, path)
		}
		// Try packed format (field "0")
		fullPath := append([]any{"0"}, path...)
		node, err := sonic.Get(row, fullPath...)
		if err == nil {
			v, _ := node.Interface()
			return v
		}
		// Try unpacked format (top level)
		node, err = sonic.Get(row, path...)
		if err == nil {
			v, _ := node.Interface()
			return v
		}
		// Field path is valid but not found: return sentinel
		return bFilters.FieldAbsent
	}

	// Legacy packed-row fallback used by older json table code paths.
	pathArr, err := table.ConvertJSONPathToArray(condKey)
	if err != nil {
		return bFilters.FieldAbsent
	}
	node, err := sonic.Get(row, pathArr...)
	if err != nil {
		if err != ast.ErrNotExist {
			log.Debugf("Sonic fetch error for path %v: %v", pathArr, err)
		}
		return bFilters.FieldAbsent
	}
	v, ierr := node.Interface()
	if ierr != nil {
		return bFilters.FieldAbsent
	}
	return v
}

func sonicLookupString(row string, condKey string) any {
	if path, ok := parseDirectPath(condKey); ok {
		if pathHasWildcard(path) {
			return lookupStringWildcard(row, path)
		}
		// Try packed format (field "0")
		fullPath := append([]any{"0"}, path...)
		node, err := sonic.GetFromString(row, fullPath...)
		if err == nil {
			v, _ := node.Interface()
			return v
		}
		// Try unpacked format (top level)
		node, err = sonic.GetFromString(row, path...)
		if err == nil {
			v, _ := node.Interface()
			return v
		}
		// Field path is valid but not found: return sentinel
		return bFilters.FieldAbsent
	}

	// Legacy packed-row fallback used by older json table code paths.
	pathArr, err := table.ConvertJSONPathToArray(condKey)
	if err != nil {
		return bFilters.FieldAbsent
	}
	node, err := sonic.GetFromString(row, pathArr...)
	if err != nil {
		if err != ast.ErrNotExist {
			log.Debugf("Sonic fetch error for path %v: %v", pathArr, err)
		}
		return bFilters.FieldAbsent
	}
	v, ierr := node.Interface()
	if ierr != nil {
		return bFilters.FieldAbsent
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
			// Try field "1" (packed), then "_id" (unpacked), then "_gid"
			for _, k := range []string{"1", "_id", "_gid"} {
				node, err := sonic.Get(row, k)
				if err == nil {
					lookupVal, _ = node.Interface()
					break
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
			// Try field "1" (packed), then "_id" (unpacked), then "_gid"
			for _, k := range []string{"1", "_id", "_gid"} {
				node, err := sonic.Get(row, k)
				if err == nil {
					lookupVal, _ = node.Interface()
					break
				}
			}
		default:
			lookupVal = f.lookupBytesCached(row, cond.Key)
		}
		return f.applyCondition(lookupVal, cond)

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
		return f.applyCondition(lookupVal, cond)

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
