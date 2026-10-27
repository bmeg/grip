// Package compiler translates a read-only subset of ISO GQL into GripQL
// traversal queries.
//
// Compilation happens in two phases, both driven by RunParser:
//
//  1. A parse-tree listener (gqlListener) walks the ANTLR parse tree and
//     records raw clause state: node/edge patterns, WHERE/ORDER BY/SKIP/LIMIT
//     clauses, and any unsupported features encountered. It does not compile.
//  2. BuildQuery validates the recorded state and assembles the *gripql.Query,
//     emitting steps in the fixed order:
//     V → label/property filters → Out/In/Both → Has (WHERE) → Sort → Render
//     → Skip → Limit.
//
// Any syntax error or unsupported feature returns an error; partial queries
// are never emitted. See endpoints/gql/COMPILER_ARCHITECTURE.md for the full
// guide, including known quirks and the extension checklist.
package compiler

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/antlr4-go/antlr/v4"
	"github.com/bmeg/grip/endpoints/gql/parser"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	//log "github.com/sirupsen/logrus"
)

// vertexSelect is a single node pattern recorded during the listener walk.
// label may hold several labels, but only label[0] is emitted (HasLabel);
// selectMap holds raw text property values that are decoded at compile time.
type vertexSelect struct {
	name      string
	label     []string
	selectMap map[string]string
}

// edgeSelect is a single edge pattern. direction is "out", "in", or "both";
// an empty label produces an unfiltered traversal step.
type edgeSelect struct {
	name      string
	label     []string
	direction string
}

// gqlListener records raw clause state from the ANTLR parse tree during the
// walk. It intentionally defers all compilation to BuildQuery; the
// inPattern/inRelation flags disambiguate the grammar's overlapping node and
// edge pattern productions, which share the same Enter* callbacks.
type gqlListener struct {
	*parser.BaseGQLListener

	queryType   string   // "MATCH" once a match statement exits; "CREATE" is rejected.
	unsupported []string // accumulated unsupported-feature markers; BuildQuery fails fast.

	vertexPath []vertexSelect // node patterns in path order.
	edgePath   []edgeSelect   // edge patterns in path order.
	returns    []string       // raw RETURN item text, in order.

	curVariable string // scratch: element variable of the pattern being walked.
	curLabels   []string
	inRelation  bool // true while inside an edge pattern; excludes edge properties from curMap.
	inPattern   bool // true while inside a node or edge pattern.

	curMap               map[string]string // raw inline property map of the current node pattern.
	currentEdgeDirection string            // set by the EnterFullEdge* callbacks.

	whereCondition parser.IValueExpressionContext // WHERE AST node, or nil if absent.
	orderExpr      string                         // raw ORDER BY clause text.
	skipExpr       string                         // raw SKIP/OFFSET clause text.
	limitExpr      string                         // raw LIMIT clause text.
}

// syntaxErrorListener collects ANTLR syntax errors as "line L:C message"
// strings so RunParser can report them all at once.
type syntaxErrorListener struct {
	*antlr.DefaultErrorListener
	errors []string
}

func (s *syntaxErrorListener) SyntaxError(_ antlr.Recognizer, _ interface{}, line, column int, msg string, _ antlr.RecognitionException) {
	s.errors = append(s.errors, fmt.Sprintf("line %d:%d %s", line, column, msg))
}

// Text-level regexes used to parse clause fragments (WHERE comparisons,
// RETURN projections, ORDER BY items) from raw ctx.GetText() strings.
// Prefer the AST when extending; these are a pragmatic shortcut for clause
// shapes the grammar does not expose as typed contexts.
var (
	simpleVariableName    = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	simpleWhereExpression = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)(=|<>|!=|<=|>=|<|>)(.+)$`)
	simpleReturnValue     = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)(?:\.([A-Za-z_][A-Za-z0-9_]*))?$`)
	simpleOrderItem       = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$`)
)

// returnProjection is one compiled RETURN item. key is the result field name
// (alias, "var.field", or the bare variable); value is the render template.
type returnProjection struct {
	key    string
	value  string
	isBare bool
}

// compiledWherePredicate is one WHERE term with explicit true and false
// branches, so NOT can be applied by swapping them and AND/OR can combine
// them with De Morgan. A nil (missing) property is forced to false on both
// branches via the Not(Eq(field, nil)) guard. Only whenTrue is emitted.
type compiledWherePredicate struct {
	variable  string
	whenTrue  *gripql.HasExpression
	whenFalse *gripql.HasExpression
}

// addVertexStep appends the filter and binding steps for one node pattern:
// HasLabel (first label only), one Has(Eq(k, v)) per inline-map entry with
// keys sorted for determinism, then As(name) — or SameAs(name) when the
// variable repeats an earlier vertex in the path.
func addVertexStep(q *gripql.Query, v vertexSelect, sameAs bool) (*gripql.Query, error) {
	if len(v.label) > 0 {
		q = q.HasLabel(v.label[0])
	}
	if len(v.selectMap) > 0 {
		keys := make([]string, 0, len(v.selectMap))
		for key := range v.selectMap {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			value, err := parseScalarLiteral(v.selectMap[key])
			if err != nil {
				return nil, fmt.Errorf("unsupported GQL features: node property literal for %q: %w", key, err)
			}
			q = q.Has(gripql.Eq(key, value))
		}
	}
	if sameAs {
		q = q.SameAs(v.name)
	} else if v.name != "" {
		q = q.As(v.name)
	}
	return q, nil
}

// decodeGQLStringLiteral decodes a GQL character string literal: quote
// doubling, backslash/control escapes, and \uXXXX/\UXXXXXX Unicode escapes.
// A leading "@" marks a no-escape string, in which case backslashes are
// preserved literally.
func decodeGQLStringLiteral(raw string) (string, error) {
	noEscape := strings.HasPrefix(raw, "@")
	if noEscape {
		raw = raw[1:]
	}
	if len(raw) < 2 || (raw[0] != '\'' && raw[0] != '"') || raw[len(raw)-1] != raw[0] {
		return "", fmt.Errorf("invalid character string literal")
	}

	quote := raw[0]
	content := raw[1 : len(raw)-1]
	var decoded strings.Builder
	decoded.Grow(len(content))
	for i := 0; i < len(content); {
		if content[i] == quote && i+1 < len(content) && content[i+1] == quote {
			decoded.WriteByte(quote)
			i += 2
			continue
		}
		if content[i] != '\\' || noEscape {
			decoded.WriteByte(content[i])
			i++
			continue
		}

		i++
		if i >= len(content) {
			return "", fmt.Errorf("incomplete character string escape")
		}
		switch content[i] {
		case '\\', '\'', '"', '`':
			decoded.WriteByte(content[i])
			i++
		case 't':
			decoded.WriteByte('\t')
			i++
		case 'b':
			decoded.WriteByte('\b')
			i++
		case 'n':
			decoded.WriteByte('\n')
			i++
		case 'r':
			decoded.WriteByte('\r')
			i++
		case 'f':
			decoded.WriteByte('\f')
			i++
		case 'u', 'U':
			digitCount := 4
			if content[i] == 'U' {
				digitCount = 6
			}
			i++
			if i+digitCount > len(content) {
				return "", fmt.Errorf("incomplete Unicode escape")
			}
			codePoint, err := strconv.ParseUint(content[i:i+digitCount], 16, 32)
			if err != nil || !utf8.ValidRune(rune(codePoint)) {
				return "", fmt.Errorf("invalid Unicode escape")
			}
			decoded.WriteRune(rune(codePoint))
			i += digitCount
		default:
			return "", fmt.Errorf("unsupported character string escape: \\%c", content[i])
		}
	}
	return decoded.String(), nil
}

// parseScalarLiteral converts a raw GQL literal into a Go value: character
// strings (via decodeGQLStringLiteral), booleans, null (nil), integers, or
// floats. Anything else is unsupported.
func parseScalarLiteral(raw string) (any, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) >= 2 {
		if raw[0] == '\'' || raw[0] == '"' || strings.HasPrefix(raw, "@") {
			return decodeGQLStringLiteral(raw)
		}
	}

	lower := strings.ToLower(raw)
	if lower == "true" {
		return true, nil
	}
	if lower == "false" {
		return false, nil
	}
	if lower == "null" {
		return nil, nil
	}

	if i, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f, nil
	}

	return nil, fmt.Errorf("unsupported literal: %s", raw)
}

// parseSimpleWhereExpression parses a "var.field op literal" comparison into
// its variable, property, and GripQL predicate. Comparison against NULL is
// rejected; use IS [NOT] NULL instead.
func parseSimpleWhereExpression(expr string) (string, string, *gripql.HasExpression, error) {
	parts := simpleWhereExpression.FindStringSubmatch(expr)
	if len(parts) != 5 {
		return "", "", nil, fmt.Errorf("unsupported GQL features: WHERE expression")
	}

	varName := parts[1]
	key := parts[2]
	op := parts[3]
	rawVal := parts[4]

	val, err := parseScalarLiteral(rawVal)
	if err != nil {
		return "", "", nil, fmt.Errorf("unsupported GQL features: WHERE literal")
	}
	if val == nil {
		return "", "", nil, fmt.Errorf("unsupported GQL features: comparison with NULL; use IS NULL or IS NOT NULL")
	}

	switch op {
	case "=":
		return varName, key, gripql.Eq(key, val), nil
	case "<>", "!=":
		return varName, key, gripql.Neq(key, val), nil
	case ">":
		return varName, key, gripql.Gt(key, val), nil
	case ">=":
		return varName, key, gripql.Gte(key, val), nil
	case "<":
		return varName, key, gripql.Lt(key, val), nil
	case "<=":
		return varName, key, gripql.Lte(key, val), nil
	default:
		return "", "", nil, fmt.Errorf("unsupported GQL features: WHERE operator")
	}
}

// compileWhereExpression compiles a WHERE value expression into a paired
// true/false predicate. Supported forms: simple comparisons, AND/OR
// composition, NOT (implemented by swapping the branches), IS [NOT] NULL,
// and parenthesized expressions.
func compileWhereExpression(expr parser.IValueExpressionContext) (*compiledWherePredicate, error) {
	switch ctx := expr.(type) {
	case *parser.ComparisonExprAltContext:
		values := ctx.AllValueExpression()
		if len(values) != 2 || ctx.CompOp() == nil {
			return nil, fmt.Errorf("unsupported GQL features: WHERE comparison")
		}
		variable, property, predicate, err := parseSimpleWhereExpression(values[0].GetText() + ctx.CompOp().GetText() + values[1].GetText())
		if err != nil {
			return nil, err
		}
		nonNull := gripql.Not(gripql.Eq(property, nil))
		return &compiledWherePredicate{
			variable:  variable,
			whenTrue:  gripql.And(nonNull, predicate),
			whenFalse: gripql.And(nonNull, gripql.Not(predicate)),
		}, nil
	case *parser.ConjunctiveExprAltContext:
		return compileCombinedWhereExpression(ctx.AllValueExpression(), false)
	case *parser.DisjunctiveExprAltContext:
		if ctx.XOR() != nil {
			return nil, fmt.Errorf("unsupported GQL features: WHERE XOR")
		}
		return compileCombinedWhereExpression(ctx.AllValueExpression(), true)
	case *parser.NotExprAltContext:
		predicate, err := compileWhereExpression(ctx.ValueExpression())
		if err != nil {
			return nil, err
		}
		return &compiledWherePredicate{
			variable:  predicate.variable,
			whenTrue:  predicate.whenFalse,
			whenFalse: predicate.whenTrue,
		}, nil
	case *parser.PredicateExprAltContext:
		predicate := ctx.Predicate()
		if predicate == nil || predicate.NullPredicate() == nil {
			return nil, fmt.Errorf("unsupported GQL features: complex WHERE predicate")
		}
		nullPredicate := predicate.NullPredicate()
		primary := nullPredicate.ValueExpressionPrimary()
		if primary == nil || nullPredicate.NullPredicatePart2() == nil {
			return nil, fmt.Errorf("unsupported GQL features: WHERE NULL predicate")
		}
		parts := simpleReturnValue.FindStringSubmatch(primary.GetText())
		if len(parts) != 3 || parts[2] == "" {
			return nil, fmt.Errorf("unsupported GQL features: WHERE NULL predicate expression")
		}
		variable := parts[1]
		property := parts[2]
		nullCheck := gripql.Eq(property, nil)
		nonNullCheck := gripql.Not(nullCheck)
		if nullPredicate.NullPredicatePart2().NOT() != nil {
			nullCheck, nonNullCheck = nonNullCheck, nullCheck
		}
		return &compiledWherePredicate{variable: variable, whenTrue: nullCheck, whenFalse: nonNullCheck}, nil
	case *parser.PrimaryExprAltContext:
		primary := ctx.ValueExpressionPrimary()
		if primary != nil && primary.ParenthesizedValueExpression() != nil {
			return compileWhereExpression(primary.ParenthesizedValueExpression().ValueExpression())
		}
	}

	return nil, fmt.Errorf("unsupported GQL features: complex WHERE expression")
}

// compileCombinedWhereExpression joins predicate terms with AND
// (disjunction=false) or OR (disjunction=true). AND combines the whenTrue
// branches with And and the whenFalse branches with Or; OR does the reverse.
// All terms must reference the same variable.
func compileCombinedWhereExpression(expressions []parser.IValueExpressionContext, disjunction bool) (*compiledWherePredicate, error) {
	if len(expressions) < 2 {
		return nil, fmt.Errorf("unsupported GQL features: WHERE expression")
	}

	var variable string
	truePredicates := make([]*gripql.HasExpression, 0, len(expressions))
	falsePredicates := make([]*gripql.HasExpression, 0, len(expressions))
	for _, expression := range expressions {
		predicate, err := compileWhereExpression(expression)
		if err != nil {
			return nil, err
		}
		if variable != "" && predicate.variable != variable {
			return nil, fmt.Errorf("unsupported GQL features: WHERE on multiple variables")
		}
		variable = predicate.variable
		truePredicates = append(truePredicates, predicate.whenTrue)
		falsePredicates = append(falsePredicates, predicate.whenFalse)
	}

	combined := &compiledWherePredicate{variable: variable}
	if disjunction {
		combined.whenTrue = gripql.Or(truePredicates...)
		combined.whenFalse = gripql.And(falsePredicates...)
	} else {
		combined.whenTrue = gripql.And(truePredicates...)
		combined.whenFalse = gripql.Or(falsePredicates...)
	}
	return combined, nil
}

// whereExpression unwraps a WHERE search condition down to its value
// expression, or returns nil if the condition is in an unsupported shape.
func whereExpression(ctx parser.ISearchConditionContext) parser.IValueExpressionContext {
	if ctx == nil || ctx.BooleanValueExpression() == nil {
		return nil
	}
	return ctx.BooleanValueExpression().ValueExpression()
}

// parseReturnProjection compiles one RETURN item. Supported: "var",
// "var.field", and each with an "AS alias". Bare variables produce isBare
// projections that render as "$var"; everything else renders "$var.field"
// under its key.
func parseReturnProjection(text string) (returnProjection, error) {
	trimmed := strings.TrimSpace(text)
	valueExpr, alias := splitReturnProjectionAlias(trimmed)

	match := simpleReturnValue.FindStringSubmatch(valueExpr)
	if len(match) != 3 {
		return returnProjection{}, fmt.Errorf("unsupported GQL features: complex RETURN projection (%q)", text)
	}

	varName := match[1]
	field := match[2]

	if !simpleVariableName.MatchString(varName) {
		return returnProjection{}, fmt.Errorf("unsupported GQL features: complex RETURN projection (%q)", text)
	}

	value := "$" + varName
	isBare := true
	key := varName

	if field != "" {
		value = "$" + varName + "." + field
		key = varName + "." + field
		isBare = false
	}

	if alias != "" {
		key = alias
		isBare = false
	}

	return returnProjection{key: key, value: value, isBare: isBare}, nil
}

// splitReturnProjectionAlias splits a RETURN item on its last " AS " keyword
// (case-insensitive) whose right side is a simple variable name. Returns the
// expression and the alias, or the original text with an empty alias.
func splitReturnProjectionAlias(text string) (string, string) {
	upper := strings.ToUpper(text)
	for i := len(text) - 2; i >= 0; i-- {
		if upper[i:i+2] != "AS" {
			continue
		}

		left := strings.TrimSpace(text[:i])
		right := strings.TrimSpace(text[i+2:])
		if left == "" || right == "" {
			continue
		}
		if simpleVariableName.MatchString(right) {
			return left, right
		}
	}

	return text, ""
}

// parseClauseUint parses a SKIP/OFFSET or LIMIT clause into an unsigned
// integer. The leading clause keyword is stripped from the raw text first;
// only plain integer literals are accepted.
func parseClauseUint(clauseText string, keyword string) (uint32, error) {
	expr := strings.TrimSpace(clauseText)
	upperKeyword := strings.ToUpper(keyword)
	upperExpr := strings.ToUpper(expr)
	if strings.HasPrefix(upperExpr, upperKeyword) {
		expr = strings.TrimSpace(expr[len(keyword):])
	}

	v, err := strconv.ParseUint(expr, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("unsupported GQL features: %s expression", upperKeyword)
	}
	return uint32(v), nil
}

// parseOrderExpression parses an ORDER BY clause into sort fields. Items are
// "var.field [ASC|DESC]"; the variable must be the current (last) traversal
// variable. Note ASC/DESC are stripped as suffixes on the raw text, so a
// property ending in those words would be mis-parsed.
func parseOrderExpression(orderText string, currentVar string) ([]*gripql.SortField, error) {
	expr := strings.ReplaceAll(strings.TrimSpace(orderText), " ", "")
	upperExpr := strings.ToUpper(expr)
	if strings.HasPrefix(upperExpr, "ORDERBY") {
		expr = strings.TrimSpace(expr[len("ORDERBY"):])
	}

	if expr == "" {
		return nil, fmt.Errorf("unsupported GQL features: ORDER BY expression")
	}

	items := strings.Split(expr, ",")
	sortFields := make([]*gripql.SortField, 0, len(items))
	for _, item := range items {
		compact := strings.TrimSpace(item)
		upperCompact := strings.ToUpper(compact)

		descending := false
		if strings.HasSuffix(upperCompact, "DESC") {
			descending = true
			compact = strings.TrimSpace(compact[:len(compact)-4])
		} else if strings.HasSuffix(upperCompact, "ASC") {
			compact = strings.TrimSpace(compact[:len(compact)-3])
		}

		parts := simpleOrderItem.FindStringSubmatch(compact)
		if len(parts) < 3 {
			return nil, fmt.Errorf("unsupported GQL features: ORDER BY item")
		}

		varName := parts[1]
		field := parts[2]
		if varName != currentVar {
			return nil, fmt.Errorf("unsupported GQL features: ORDER BY on non-current variable")
		}

		sortFields = append(sortFields, &gripql.SortField{Field: field, Descending: descending})
	}

	return sortFields, nil
}

// markUnsupported records a feature the compiler cannot translate. Duplicates
// are ignored; BuildQuery reports all of them as a single error.
func (c *gqlListener) markUnsupported(feature string) {
	for _, v := range c.unsupported {
		if v == feature {
			return
		}
	}
	c.unsupported = append(c.unsupported, feature)
}

// BuildQuery validates the recorded clause state and assembles the GripQL
// query. Step order is fixed and significant: V → filters/traversal →
// Has (WHERE) → Sort → Render → Skip → Limit. WHERE and ORDER BY must
// reference the last (current) path variable.
func (c *gqlListener) BuildQuery() (*gripql.Query, error) {
	if len(c.unsupported) > 0 {
		return nil, fmt.Errorf("unsupported GQL features: %s", strings.Join(c.unsupported, ", "))
	}

	if c.queryType == "MATCH" {
		if len(c.vertexPath) == 0 {
			return nil, fmt.Errorf("invalid query: MATCH clause did not include any node patterns")
		}
		if len(c.edgePath) > 0 && len(c.vertexPath) != len(c.edgePath)+1 {
			return nil, fmt.Errorf("invalid query: path length mismatch (vertices=%d, edges=%d)", len(c.vertexPath), len(c.edgePath))
		}
		seenVariables := make(map[string]struct{}, len(c.vertexPath))
		repeatedVariables := make(map[string]bool, len(c.vertexPath))
		for _, vertex := range c.vertexPath {
			if vertex.name == "" {
				continue
			}
			if _, exists := seenVariables[vertex.name]; exists {
				repeatedVariables[vertex.name] = true
				continue
			}
			seenVariables[vertex.name] = struct{}{}
		}
		seenEdgeVariables := make(map[string]struct{}, len(c.edgePath))
		for _, edge := range c.edgePath {
			if edge.name == "" {
				continue
			}
			if _, exists := seenVariables[edge.name]; exists {
				return nil, fmt.Errorf("unsupported GQL features: variable %q is used for both node and edge patterns", edge.name)
			}
			if _, exists := seenEdgeVariables[edge.name]; exists {
				return nil, fmt.Errorf("unsupported GQL features: repeated edge variable %q", edge.name)
			}
			seenEdgeVariables[edge.name] = struct{}{}
		}
		q := gripql.NewQuery()
		q = q.V()
		var err error
		q, err = addVertexStep(q, c.vertexPath[0], false)
		if err != nil {
			return nil, err
		}

		for i := 0; i < len(c.edgePath); i++ {
			e := c.edgePath[i]
			switch e.direction {
			case "out":
				q = q.Out(e.label...)
			case "in":
				q = q.In(e.label...)
			case "both":
				q = q.Both(e.label...)
			default:
				return nil, fmt.Errorf("unsupported GQL features: relationship direction")
			}
			nextVertex := c.vertexPath[i+1]
			q, err = addVertexStep(q, nextVertex, repeatedVariables[nextVertex.name])
			if err != nil {
				return nil, err
			}
		}

		if c.whereCondition != nil {
			wherePredicate, err := compileWhereExpression(c.whereCondition)
			if err != nil {
				return nil, err
			}
			currentVar := c.vertexPath[len(c.vertexPath)-1].name
			if currentVar == "" || wherePredicate.variable != currentVar {
				return nil, fmt.Errorf("unsupported GQL features: WHERE on non-current variable")
			}
			q = q.Has(wherePredicate.whenTrue)
		}

		if c.orderExpr != "" {
			currentVar := c.vertexPath[len(c.vertexPath)-1].name
			if currentVar == "" {
				return nil, fmt.Errorf("unsupported GQL features: ORDER BY without current variable")
			}
			sortFields, err := parseOrderExpression(c.orderExpr, currentVar)
			if err != nil {
				return nil, err
			}
			q = q.Sort(sortFields)
		}

		if len(c.returns) > 0 {
			projections := make([]returnProjection, 0, len(c.returns))
			for _, item := range c.returns {
				p, err := parseReturnProjection(item)
				if err != nil {
					return nil, err
				}
				projections = append(projections, p)
			}

			if len(projections) == 1 && projections[0].isBare {
				q = q.Render(projections[0].value)
			} else {
				renderMap := map[string]any{}
				for _, p := range projections {
					renderMap[p.key] = p.value
				}
				q = q.Render(renderMap)
			}
		}

		if c.skipExpr != "" {
			skipVal, err := parseClauseUint(c.skipExpr, "SKIP")
			if err != nil {
				return nil, err
			}
			q = q.Skip(skipVal)
		}

		if c.limitExpr != "" {
			limitVal, err := parseClauseUint(c.limitExpr, "LIMIT")
			if err != nil {
				return nil, err
			}
			q = q.Limit(limitVal)
		}
		log.Debugf("Query: %s", q.String())
		return q, nil
	} else if c.queryType == "CREATE" {
		return nil, fmt.Errorf("unsupported GQL features: CREATE")
	}
	return nil, fmt.Errorf("unknown query type")
}

func (c *gqlListener) EnterStatement(ctx *parser.StatementContext) {
	log.Debugf("Entering Statement %#v", ctx.GetText())
}

func (c *gqlListener) EnterMatchStatement(ctx *parser.MatchStatementContext) {
	log.Debugf("Is Match")
	c.vertexPath = make([]vertexSelect, 0, 10)
	c.edgePath = make([]edgeSelect, 0, 10)
	c.returns = nil
	c.whereCondition = nil
	c.orderExpr = ""
	c.skipExpr = ""
	c.limitExpr = ""
}

func (c *gqlListener) ExitMatchStatement(ctx *parser.MatchStatementContext) {
	log.Debugf("Building Query: %#v", c.vertexPath)
	c.queryType = "MATCH"
}

// The callbacks below recognize grammar productions the compiler does not
// implement and mark the whole query unsupported. This keeps rejections
// explicit and keeps the error messages stable for tests and callers.
func (c *gqlListener) EnterCreateSchemaStatement(ctx *parser.CreateSchemaStatementContext) {
	c.markUnsupported("CREATE")
}

func (c *gqlListener) EnterCreateGraphStatement(ctx *parser.CreateGraphStatementContext) {
	c.markUnsupported("CREATE")
}

func (c *gqlListener) EnterCreateGraphTypeStatement(ctx *parser.CreateGraphTypeStatementContext) {
	c.markUnsupported("CREATE")
}

// EnterNodePattern and ExitNodePattern delimit one node pattern. The
// inPattern flag scopes the shared ElementVariable/LabelName/
// PropertyKeyValuePair callbacks to the pattern being walked.
func (c *gqlListener) EnterNodePattern(ctx *parser.NodePatternContext) {
	log.Debugf("NodePattern: %s", ctx.GetText())
	c.inPattern = true
	c.inRelation = false
	c.curVariable = ""
	c.curLabels = []string{}
	c.curMap = map[string]string{}
}

func (c *gqlListener) ExitNodePattern(ctx *parser.NodePatternContext) {
	vs := vertexSelect{name: c.curVariable, label: c.curLabels}
	if len(c.curMap) > 0 {
		vs.selectMap = c.curMap
	}
	c.vertexPath = append(c.vertexPath, vs)
	c.inPattern = false
}

func (c *gqlListener) EnterElementVariable(ctx *parser.ElementVariableContext) {
	if !c.inPattern {
		return
	}
	log.Debugf("Variable: %s", ctx.GetText())
	c.curVariable = ctx.GetText()
}

func (c *gqlListener) EnterPropertyKeyValuePair(ctx *parser.PropertyKeyValuePairContext) {
	if !c.inPattern || c.inRelation {
		return
	}
	key, value, ok := strings.Cut(ctx.GetText(), ":")
	if !ok || key == "" || value == "" {
		c.markUnsupported("property map")
		return
	}
	c.curMap[key] = value
}

// EnterEdgePattern delimits one edge pattern. inRelation additionally
// suppresses property-map collection, since edge property maps are
// intentionally unsupported.
func (c *gqlListener) EnterEdgePattern(ctx *parser.EdgePatternContext) {
	log.Debugf("EdgePattern: %s", ctx.GetText())
	c.inPattern = true
	c.inRelation = true
	c.curVariable = ""
	c.curLabels = []string{}
	c.curMap = map[string]string{}
	c.currentEdgeDirection = "both"
}

// The EnterFullEdge* callbacks map the edge direction tokens to traversal
// directions. The hybrid grammar alternatives (LeftOrUndirected,
// UndirectedOrRight, LeftOrRight) are deliberately unsupported.
func (c *gqlListener) EnterFullEdgePointingLeft(ctx *parser.FullEdgePointingLeftContext) {
	c.currentEdgeDirection = "in"
	_ = ctx
}

func (c *gqlListener) EnterFullEdgePointingRight(ctx *parser.FullEdgePointingRightContext) {
	c.currentEdgeDirection = "out"
	_ = ctx
}

func (c *gqlListener) EnterFullEdgeUndirected(ctx *parser.FullEdgeUndirectedContext) {
	c.currentEdgeDirection = "both"
	_ = ctx
}

func (c *gqlListener) EnterFullEdgeLeftOrUndirected(ctx *parser.FullEdgeLeftOrUndirectedContext) {
	c.markUnsupported("relationship direction")
	_ = ctx
}

func (c *gqlListener) EnterFullEdgeUndirectedOrRight(ctx *parser.FullEdgeUndirectedOrRightContext) {
	c.markUnsupported("relationship direction")
	_ = ctx
}

func (c *gqlListener) EnterFullEdgeLeftOrRight(ctx *parser.FullEdgeLeftOrRightContext) {
	c.markUnsupported("relationship direction")
	_ = ctx
}

func (c *gqlListener) EnterFullEdgeAnyDirection(ctx *parser.FullEdgeAnyDirectionContext) {
	c.currentEdgeDirection = "both"
	_ = ctx
}

func (c *gqlListener) ExitEdgePattern(ctx *parser.EdgePatternContext) {
	e := edgeSelect{name: c.curVariable, label: c.curLabels, direction: c.currentEdgeDirection}
	log.Debugf("RelationshipPattern: %s", e)
	c.edgePath = append(c.edgePath, e)
	c.inPattern = false
	c.inRelation = false
	_ = ctx
}

func (c *gqlListener) EnterLabelName(ctx *parser.LabelNameContext) {
	if !c.inPattern {
		return
	}
	log.Debugf("Label: %s", ctx.GetText())
	c.curLabels = append(c.curLabels, ctx.GetText())
}

func (c *gqlListener) EnterReturnStatement(ctx *parser.ReturnStatementContext) {
	log.Debugf("Returns: %s", ctx.GetText())
	c.returns = []string{}
}

func (c *gqlListener) EnterReturnItem(ctx *parser.ReturnItemContext) {
	log.Debugf("Return Projections: %s", ctx.GetText())
	c.returns = append(c.returns, ctx.GetText())
}

func (c *gqlListener) EnterWhereClause(ctx *parser.WhereClauseContext) {
	c.whereCondition = whereExpression(ctx.SearchCondition())
}

func (c *gqlListener) EnterGraphPatternWhereClause(ctx *parser.GraphPatternWhereClauseContext) {
	if c.whereCondition == nil {
		c.whereCondition = whereExpression(ctx.SearchCondition())
	}
}

func (c *gqlListener) EnterElementPatternWhereClause(ctx *parser.ElementPatternWhereClauseContext) {
	if c.whereCondition == nil {
		c.whereCondition = whereExpression(ctx.SearchCondition())
	}
}

func (c *gqlListener) EnterInsertStatement(ctx *parser.InsertStatementContext) {
	c.markUnsupported("CREATE")
}

func (c *gqlListener) EnterSetStatement(ctx *parser.SetStatementContext) {
	c.markUnsupported("SET")
	_ = ctx
}

func (c *gqlListener) EnterDeleteStatement(ctx *parser.DeleteStatementContext) {
	c.markUnsupported("DELETE")
	_ = ctx
}

func (c *gqlListener) EnterRemoveStatement(ctx *parser.RemoveStatementContext) {
	c.markUnsupported("REMOVE")
	_ = ctx
}

func (c *gqlListener) EnterLetStatement(ctx *parser.LetStatementContext) {
	c.markUnsupported("WITH")
	_ = ctx
}

func (c *gqlListener) EnterForStatement(ctx *parser.ForStatementContext) {
	c.markUnsupported("FOR")
	_ = ctx
}

func (c *gqlListener) EnterQueryConjunction(ctx *parser.QueryConjunctionContext) {
	c.markUnsupported("UNION")
	_ = ctx
}

func (c *gqlListener) EnterSelectStatement(ctx *parser.SelectStatementContext) {
	c.markUnsupported("SELECT")
	_ = ctx
}

func (c *gqlListener) EnterCallProcedureStatement(ctx *parser.CallProcedureStatementContext) {
	c.markUnsupported("procedure call")
	_ = ctx
}

func (c *gqlListener) EnterOptionalMatchStatement(ctx *parser.OptionalMatchStatementContext) {
	c.markUnsupported("OPTIONAL MATCH")
	_ = ctx
}

func (c *gqlListener) EnterGroupByClause(ctx *parser.GroupByClauseContext) {
	c.markUnsupported("GROUP BY")
	_ = ctx
}

func (c *gqlListener) EnterGraphPatternYieldClause(ctx *parser.GraphPatternYieldClauseContext) {
	c.markUnsupported("YIELD")
	_ = ctx
}

func (c *gqlListener) EnterPathVariableDeclaration(ctx *parser.PathVariableDeclarationContext) {
	c.markUnsupported("path variables")
	_ = ctx
}

func (c *gqlListener) EnterKeepClause(ctx *parser.KeepClauseContext) {
	c.markUnsupported("KEEP")
	_ = ctx
}

func (c *gqlListener) EnterPathPatternPrefix(ctx *parser.PathPatternPrefixContext) {
	c.markUnsupported("path search")
	_ = ctx
}

func (c *gqlListener) EnterGraphPatternQuantifier(ctx *parser.GraphPatternQuantifierContext) {
	c.markUnsupported("variable-length relationships")
	_ = ctx
}

func (c *gqlListener) EnterFixedQuantifier(ctx *parser.FixedQuantifierContext) {
	c.markUnsupported("variable-length relationships")
	_ = ctx
}

func (c *gqlListener) EnterGeneralQuantifier(ctx *parser.GeneralQuantifierContext) {
	c.markUnsupported("variable-length relationships")
	_ = ctx
}

// EnterFilterStatement marks FILTER unsupported, except for text that already
// starts with "FILTERWHERE" (e.g. "FILTER WHERE ..."), which falls through to
// the WHERE compiler and is rejected there with a WHERE error instead.
func (c *gqlListener) EnterFilterStatement(ctx *parser.FilterStatementContext) {
	upper := strings.ToUpper(ctx.GetText())
	if !strings.HasPrefix(upper, "FILTERWHERE") {
		c.markUnsupported("FILTER")
	}
}

func (c *gqlListener) EnterOrderByClause(ctx *parser.OrderByClauseContext) {
	c.orderExpr = ctx.GetText()
}

func (c *gqlListener) EnterOffsetClause(ctx *parser.OffsetClauseContext) {
	c.skipExpr = ctx.GetText()
}

func (c *gqlListener) EnterLimitClause(ctx *parser.LimitClauseContext) {
	c.limitExpr = ctx.GetText()
}

// RunParser is the public entry point: it lexes and parses the GQL text,
// walks the tree with gqlListener, and returns the compiled query. Any
// syntax error, unsupported feature, or invalid query structure returns an
// error; partial queries are never emitted.
func RunParser(gqlText string) (*gripql.Query, error) {
	// Setup the input
	is := antlr.NewInputStream(gqlText)
	errorListener := &syntaxErrorListener{DefaultErrorListener: antlr.NewDefaultErrorListener()}
	// Create the Lexer
	lexer := parser.NewGQLLexer(is)
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(errorListener)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	// Create the Parser
	p := parser.NewGQLParser(stream)
	p.RemoveErrorListeners()
	p.AddErrorListener(errorListener)
	cl := &gqlListener{BaseGQLListener: &parser.BaseGQLListener{}}
	// Finally parse the expression
	tree := p.GqlProgram()
	antlr.ParseTreeWalkerDefault.Walk(cl, tree)

	if len(errorListener.errors) > 0 {
		return nil, fmt.Errorf("parse error: %s", strings.Join(errorListener.errors, "; "))
	}

	return cl.BuildQuery()
}
