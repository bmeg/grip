package compiler

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/bmeg/grip/endpoints/gql/parser"

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

type gqlListener struct {
	*parser.BaseGQLListener

	queryType string

	unsupported []string

	vertexPath []vertexSelect
	edgePath   []edgeSelect
	returns    []string

	curVariable string
	curLabels   []string
	inRelation  bool
	inPattern   bool

	curMap               map[string]string
	currentEdgeDirection string

	whereExpr string
	orderExpr string
	skipExpr  string
	limitExpr string
}

type syntaxErrorListener struct {
	*antlr.DefaultErrorListener
	errors []string
}

func (s *syntaxErrorListener) SyntaxError(_ antlr.Recognizer, _ interface{}, line, column int, msg string, _ antlr.RecognitionException) {
	s.errors = append(s.errors, fmt.Sprintf("line %d:%d %s", line, column, msg))
}

func evalHasExpression(key string, exp string) *gripql.HasExpression {
	if strings.HasPrefix(exp, "'") && strings.HasSuffix(exp, "'") {
		exp = exp[1 : len(exp)-1]
	}
	return gripql.Eq(key, exp)
}

var simpleVariableName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var simpleWhereExpression = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)(=|<>|!=|<=|>=|<|>)(.+)$`)
var simpleReturnValue = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)(?:\.([A-Za-z_][A-Za-z0-9_]*))?$`)
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
		return "", nil, fmt.Errorf("unsupported GQL features: complex WHERE expression")
	}

	parts := simpleWhereExpression.FindStringSubmatch(expr)
	if len(parts) != 5 {
		return "", nil, fmt.Errorf("unsupported GQL features: WHERE expression")
	}

	varName := parts[1]
	key := parts[2]
	op := parts[3]
	rawVal := parts[4]

	val, err := parseScalarLiteral(rawVal)
	if err != nil {
		return "", nil, fmt.Errorf("unsupported GQL features: WHERE literal")
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
		return "", nil, fmt.Errorf("unsupported GQL features: WHERE operator")
	}
}

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

func (c *gqlListener) markUnsupported(feature string) {
	for _, v := range c.unsupported {
		if v == feature {
			return
		}
	}
	c.unsupported = append(c.unsupported, feature)
}

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
				return nil, fmt.Errorf("unsupported GQL features: relationship direction")
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
				return nil, fmt.Errorf("unsupported GQL features: WHERE on non-current variable")
			}
			q = q.Has(whereExpr)
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
	c.whereExpr = ""
	c.orderExpr = ""
	c.skipExpr = ""
	c.limitExpr = ""
}

func (c *gqlListener) ExitMatchStatement(ctx *parser.MatchStatementContext) {
	log.Debugf("Building Query: %#v", c.vertexPath)
	c.queryType = "MATCH"
}

func (c *gqlListener) EnterCreateSchemaStatement(ctx *parser.CreateSchemaStatementContext) {
	c.markUnsupported("CREATE")
}

func (c *gqlListener) EnterCreateGraphStatement(ctx *parser.CreateGraphStatementContext) {
	c.markUnsupported("CREATE")
}

func (c *gqlListener) EnterCreateGraphTypeStatement(ctx *parser.CreateGraphTypeStatementContext) {
	c.markUnsupported("CREATE")
}

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

func (c *gqlListener) EnterEdgePattern(ctx *parser.EdgePatternContext) {
	log.Debugf("EdgePattern: %s", ctx.GetText())
	c.inPattern = true
	c.inRelation = true
	c.curVariable = ""
	c.curLabels = []string{}
	c.curMap = map[string]string{}
	c.currentEdgeDirection = "both"
}

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
	c.whereExpr = ctx.GetText()
}

func (c *gqlListener) EnterGraphPatternWhereClause(ctx *parser.GraphPatternWhereClauseContext) {
	if c.whereExpr == "" {
		c.whereExpr = ctx.GetText()
	}
}

func (c *gqlListener) EnterElementPatternWhereClause(ctx *parser.ElementPatternWhereClauseContext) {
	if c.whereExpr == "" {
		c.whereExpr = ctx.GetText()
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
