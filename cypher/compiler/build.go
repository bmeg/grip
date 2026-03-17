package compiler

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/bmeg/grip/cypher/parser"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	//log "github.com/sirupsen/logrus"
)

type vertexSelect struct {
	name      string
	label     []string
	selectMap map[string]string
}

type edgeSelect struct {
	name      string
	label     []string
	direction string
}

type cypherListener struct {
	*parser.BaseCypherListener

	queryType string

	unsupported []string

	vertexPath []vertexSelect
	edgePath   []edgeSelect
	returns    []string

	curVariable string
	curLabels   []string
	inRelation  bool

	curMapKey []string

	curExpression []string

	curMap map[string]string

	whereExpr string
	orderExpr string
	skipExpr  string
	limitExpr string
}

func evalHasExpression(key string, exp string) *gripql.HasExpression {
	if strings.HasPrefix(exp, "'") && strings.HasSuffix(exp, "'") {
		exp = exp[1 : len(exp)-1]
	}
	return gripql.Eq(key, exp)
}

var simpleVariableName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var simpleWhereExpression = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)(=|<>|!=|<=|>=|<|>)(.+)$`)
var simpleReturnProjection = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)(?:\.([A-Za-z_][A-Za-z0-9_]*))?(?:\s+(?i:AS)\s+([A-Za-z_][A-Za-z0-9_]*))?$`)
var simpleOrderItem = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$`)

type returnProjection struct {
	key    string
	value  string
	isBare bool
}

func addVertexStep(q *gripql.Query, v vertexSelect) *gripql.Query {
	if len(v.label) > 0 {
		q = q.HasLabel(v.label[0])
	}
	if len(v.selectMap) > 0 {
		for k, val := range v.selectMap {
			e := evalHasExpression(k, val)
			q = q.Has(e)
		}
	}
	if v.name != "" {
		q = q.As(v.name)
	}
	return q
}

func parseScalarLiteral(raw string) (any, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) >= 2 {
		if (raw[0] == '\'' && raw[len(raw)-1] == '\'') || (raw[0] == '"' && raw[len(raw)-1] == '"') {
			return raw[1 : len(raw)-1], nil
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

func parseWhereExpression(whereText string) (string, *gripql.HasExpression, error) {
	expr := strings.TrimSpace(whereText)
	if strings.HasPrefix(strings.ToUpper(expr), "WHERE") {
		expr = strings.TrimSpace(expr[5:])
	}

	if strings.Contains(expr, " AND ") || strings.Contains(expr, " OR ") {
		return "", nil, fmt.Errorf("unsupported cypher features: complex WHERE expression")
	}

	parts := simpleWhereExpression.FindStringSubmatch(expr)
	if len(parts) != 5 {
		return "", nil, fmt.Errorf("unsupported cypher features: WHERE expression")
	}

	varName := parts[1]
	key := parts[2]
	op := parts[3]
	rawVal := parts[4]

	val, err := parseScalarLiteral(rawVal)
	if err != nil {
		return "", nil, fmt.Errorf("unsupported cypher features: WHERE literal")
	}

	switch op {
	case "=":
		return varName, gripql.Eq(key, val), nil
	case "<>", "!=":
		return varName, gripql.Neq(key, val), nil
	case ">":
		return varName, gripql.Gt(key, val), nil
	case ">=":
		return varName, gripql.Gte(key, val), nil
	case "<":
		return varName, gripql.Lt(key, val), nil
	case "<=":
		return varName, gripql.Lte(key, val), nil
	default:
		return "", nil, fmt.Errorf("unsupported cypher features: WHERE operator")
	}
}

func parseReturnProjection(text string) (returnProjection, error) {
	match := simpleReturnProjection.FindStringSubmatch(strings.TrimSpace(text))
	if len(match) != 4 {
		return returnProjection{}, fmt.Errorf("unsupported cypher features: complex RETURN projection (%q)", text)
	}

	varName := match[1]
	field := match[2]
	alias := match[3]

	if !simpleVariableName.MatchString(varName) {
		return returnProjection{}, fmt.Errorf("unsupported cypher features: complex RETURN projection (%q)", text)
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

func parseClauseUint(clauseText string, keyword string) (uint32, error) {
	expr := strings.TrimSpace(clauseText)
	upperKeyword := strings.ToUpper(keyword)
	upperExpr := strings.ToUpper(expr)
	if strings.HasPrefix(upperExpr, upperKeyword) {
		expr = strings.TrimSpace(expr[len(keyword):])
	}

	v, err := strconv.ParseUint(expr, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("unsupported cypher features: %s expression", upperKeyword)
	}
	return uint32(v), nil
}

func parseOrderExpression(orderText string, currentVar string) ([]*gripql.SortField, error) {
	expr := strings.ReplaceAll(strings.TrimSpace(orderText), " ", "")
	upperExpr := strings.ToUpper(expr)
	if strings.HasPrefix(upperExpr, "ORDERBY") {
		expr = strings.TrimSpace(expr[len("ORDERBY"):])
	}

	if expr == "" {
		return nil, fmt.Errorf("unsupported cypher features: ORDER BY expression")
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
			return nil, fmt.Errorf("unsupported cypher features: ORDER BY item")
		}

		varName := parts[1]
		field := parts[2]
		if varName != currentVar {
			return nil, fmt.Errorf("unsupported cypher features: ORDER BY on non-current variable")
		}

		sortFields = append(sortFields, &gripql.SortField{Field: field, Descending: descending})
	}

	return sortFields, nil
}

func (c *cypherListener) markUnsupported(feature string) {
	for _, v := range c.unsupported {
		if v == feature {
			return
		}
	}
	c.unsupported = append(c.unsupported, feature)
}

func (c *cypherListener) BuildQuery() (*gripql.Query, error) {
	if len(c.unsupported) > 0 {
		return nil, fmt.Errorf("unsupported cypher features: %s", strings.Join(c.unsupported, ", "))
	}

	if c.queryType == "MATCH" {
		if len(c.vertexPath) == 0 {
			return nil, fmt.Errorf("invalid query: MATCH clause did not include any node patterns")
		}
		if len(c.edgePath) > 0 && len(c.vertexPath) != len(c.edgePath)+1 {
			return nil, fmt.Errorf("invalid query: path length mismatch (vertices=%d, edges=%d)", len(c.vertexPath), len(c.edgePath))
		}
		q := gripql.NewQuery()
		q = q.V()
		q = addVertexStep(q, c.vertexPath[0])

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
				return nil, fmt.Errorf("unsupported cypher features: relationship direction")
			}
			q = addVertexStep(q, c.vertexPath[i+1])
		}

		if c.whereExpr != "" {
			whereVar, whereExpr, err := parseWhereExpression(c.whereExpr)
			if err != nil {
				return nil, err
			}
			currentVar := c.vertexPath[len(c.vertexPath)-1].name
			if currentVar == "" || whereVar != currentVar {
				return nil, fmt.Errorf("unsupported cypher features: WHERE on non-current variable")
			}
			q = q.Has(whereExpr)
		}

		if c.orderExpr != "" {
			currentVar := c.vertexPath[len(c.vertexPath)-1].name
			if currentVar == "" {
				return nil, fmt.Errorf("unsupported cypher features: ORDER BY without current variable")
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
		return nil, fmt.Errorf("unsupported cypher features: CREATE")
	}
	return nil, fmt.Errorf("unknown query type")
}

func (c *cypherListener) EnterOC_Statement(ctx *parser.OC_StatementContext) {
	log.Debugf("Entering Statement %#v", ctx.GetText())
}

func (c *cypherListener) EnterOC_Match(ctx *parser.OC_MatchContext) {
	log.Debugf("Is Match")
	c.vertexPath = make([]vertexSelect, 0, 10)
	c.edgePath = make([]edgeSelect, 0, 10)
	c.whereExpr = ""
	c.orderExpr = ""
	c.skipExpr = ""
	c.limitExpr = ""
}

func (c *cypherListener) ExitOC_Match(ctx *parser.OC_MatchContext) {
	log.Debugf("Building Query: %#v", c.vertexPath)
	c.queryType = "MATCH"
}

func (c *cypherListener) EnterOC_Create(ctx *parser.OC_CreateContext) {
	c.markUnsupported("CREATE")
}

func (c *cypherListener) ExitOC_Create(ctx *parser.OC_CreateContext) {
	log.Debugf("Building Query: %#v", c.vertexPath)
	c.queryType = "CREATE"
}

func (c *cypherListener) EnterOC_PatternElement(ctx *parser.OC_PatternElementContext) {
	log.Debugf("Is pattern %s", ctx.GetText())
}

func (c *cypherListener) EnterOC_NodePattern(ctx *parser.OC_NodePatternContext) {
	log.Debugf("NodePattern: %s", ctx.GetText())
	c.inRelation = false
	c.curVariable = ""
	c.curLabels = []string{}
	c.curMap = map[string]string{}
}

func (c *cypherListener) ExitOC_NodePattern(ctx *parser.OC_NodePatternContext) {
	vs := vertexSelect{name: c.curVariable, label: c.curLabels}
	if len(c.curMap) > 0 {
		vs.selectMap = c.curMap
	}
	c.vertexPath = append(c.vertexPath, vs)
}

func (c *cypherListener) EnterOC_Variable(ctx *parser.OC_VariableContext) {
	log.Debugf("Variable: %s", ctx.GetText())
	c.curVariable = ctx.GetText()
}

func (c *cypherListener) EnterOC_MapLiteral(ctx *parser.OC_MapLiteralContext) {
	log.Debugf("MapLiteral: %s", ctx.GetText())
	c.curMapKey = []string{}
	c.curExpression = []string{}
}

func (c *cypherListener) ExitOC_MapLiteral(ctx *parser.OC_MapLiteralContext) {
	out := map[string]string{}
	for i := 0; i < len(c.curMapKey) && i < len(c.curExpression); i++ {
		out[c.curMapKey[i]] = c.curExpression[i]
	}
	c.curMap = out
}

func (c *cypherListener) EnterOC_PropertyKeyName(ctx *parser.OC_PropertyKeyNameContext) {
	c.curMapKey = append(c.curMapKey, ctx.GetText())
}

func (c *cypherListener) EnterOC_Expression(ctx *parser.OC_ExpressionContext) {
	log.Debugf("Expression: %s", ctx.GetText())
	c.curExpression = append(c.curExpression, ctx.GetText())
}

func (c *cypherListener) EnterOC_RelationshipPattern(ctx *parser.OC_RelationshipPatternContext) {
	log.Debugf("RelationshipPattern: %s", ctx.GetText())
	c.inRelation = true
	c.curVariable = ""
	c.curLabels = []string{}
}

func (c *cypherListener) ExitOC_RelationshipPattern(ctx *parser.OC_RelationshipPatternContext) {
	direction := "both"
	if ctx.OC_RightArrowHead() != nil && ctx.OC_LeftArrowHead() == nil {
		direction = "out"
	}
	if ctx.OC_LeftArrowHead() != nil && ctx.OC_RightArrowHead() == nil {
		direction = "in"
	}
	e := edgeSelect{name: c.curVariable, label: c.curLabels, direction: direction}
	log.Debugf("RelationshipPattern: %s", e)
	c.edgePath = append(c.edgePath, e)
	c.inRelation = false
}

func (c *cypherListener) EnterOC_LabelName(ctx *parser.OC_LabelNameContext) {
	if c.inRelation {
		return
	}
	log.Debugf("Label: %s", ctx.GetText())
	c.curLabels = append(c.curLabels, ctx.GetText())
}

func (c *cypherListener) EnterOC_RelTypeName(ctx *parser.OC_RelTypeNameContext) {
	if !c.inRelation {
		return
	}
	log.Debugf("Relationship Label: %s", ctx.GetText())
	c.curLabels = append(c.curLabels, ctx.GetText())
}

func (c *cypherListener) EnterOC_Return(ctx *parser.OC_ReturnContext) {
	log.Debugf("Returns: %s", ctx.GetText())
	c.returns = []string{}
}

func (c *cypherListener) EnterOC_ProjectionItem(ctx *parser.OC_ProjectionItemContext) {
	log.Debugf("Return Projections: %s", ctx.GetText())
	c.returns = append(c.returns, ctx.GetText())
}

func (c *cypherListener) EnterOC_Where(ctx *parser.OC_WhereContext) {
	c.whereExpr = ctx.GetText()
}

func (c *cypherListener) EnterOC_With(ctx *parser.OC_WithContext) {
	c.markUnsupported("WITH")
}

func (c *cypherListener) EnterOC_Set(ctx *parser.OC_SetContext) {
	c.markUnsupported("SET")
}

func (c *cypherListener) EnterOC_Delete(ctx *parser.OC_DeleteContext) {
	c.markUnsupported("DELETE")
}

func (c *cypherListener) EnterOC_Remove(ctx *parser.OC_RemoveContext) {
	c.markUnsupported("REMOVE")
}

func (c *cypherListener) EnterOC_Merge(ctx *parser.OC_MergeContext) {
	c.markUnsupported("MERGE")
}

func (c *cypherListener) EnterOC_Unwind(ctx *parser.OC_UnwindContext) {
	c.markUnsupported("UNWIND")
}

func (c *cypherListener) EnterOC_Union(ctx *parser.OC_UnionContext) {
	c.markUnsupported("UNION")
}

func (c *cypherListener) EnterOC_MultiPartQuery(ctx *parser.OC_MultiPartQueryContext) {
	c.markUnsupported("multi-part query")
}

func (c *cypherListener) EnterOC_Order(ctx *parser.OC_OrderContext) {
	c.orderExpr = ctx.GetText()
}

func (c *cypherListener) EnterOC_Skip(ctx *parser.OC_SkipContext) {
	c.skipExpr = ctx.GetText()
}

func (c *cypherListener) EnterOC_Limit(ctx *parser.OC_LimitContext) {
	c.limitExpr = ctx.GetText()
}

func (c *cypherListener) EnterOC_RangeLiteral(ctx *parser.OC_RangeLiteralContext) {
	c.markUnsupported("variable-length relationships")
}

func RunParser(oc string) (*gripql.Query, error) {
	// Setup the input
	is := antlr.NewInputStream(oc)
	// Create the Lexer
	lexer := parser.NewCypherLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	// Create the Parser
	p := parser.NewCypherParser(stream)
	cl := &cypherListener{}
	// Finally parse the expression
	antlr.ParseTreeWalkerDefault.Walk(cl, p.OC_Cypher())

	return cl.BuildQuery()
}
