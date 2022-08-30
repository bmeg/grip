package cypher

import (
	"fmt"
	"strings"

	"github.com/antlr/antlr4/runtime/Go/antlr"
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
	name  string
	label []string
}

type cypherListener struct {
	*parser.BaseCypherListener

	queryType string

	vertexPath []vertexSelect
	edgePath   []edgeSelect
	returns    []string

	curVariable string
	curLabels   []string

	curMapKey []string

	curExpression []string

	curMap map[string]string
}

func evalHasExpression(key string, exp string) *gripql.HasExpression {
	if strings.HasPrefix(exp, "'") && strings.HasSuffix(exp, "'") {
		exp = exp[1 : len(exp)-1]
	}
	return gripql.Eq(key, exp)
}

func (c *cypherListener) BuildQuery() (*gripql.Query, error) {
	if c.queryType == "MATCH" {
		q := gripql.NewQuery()
		q = q.V()
		if len(c.vertexPath) > 0 && len(c.vertexPath[0].label) > 0 {
			q = q.HasLabel(c.vertexPath[0].label[0])
		}
		for i := 0; i < len(c.vertexPath); i++ {
			if len(c.vertexPath[i].selectMap) > 0 {
				for k, v := range c.vertexPath[i].selectMap {
					e := evalHasExpression(k, v)
					q = q.Has(e)
				}
			}
			q = q.As(c.vertexPath[i].name)
		}
		if len(c.returns) > 0 {
			q = q.Render("$" + c.returns[0])
		}
		log.Debugf("Query: %s", q.String())
		return q, nil
	} else if c.queryType == "CREATE" {
		log.Debugf("Query Build: %#v", c)
	}
	return nil, fmt.Errorf("Unknown query type")
}

func (c *cypherListener) EnterOC_Statement(ctx *parser.OC_StatementContext) {
	log.Debugf("Entering Statement %#v", ctx.GetText())
}

func (c *cypherListener) EnterOC_Match(ctx *parser.OC_MatchContext) {
	log.Debugf("Is Match")
	c.vertexPath = make([]vertexSelect, 0, 10)
	c.edgePath = make([]edgeSelect, 0, 10)
}

func (c *cypherListener) ExitOC_Match(ctx *parser.OC_MatchContext) {
	log.Debugf("Building Query: %#v", c.vertexPath)
	c.queryType = "MATCH"
}

func (c *cypherListener) EnterOC_Create(ctx *parser.OC_CreateContext) {
	log.Debugf("Is Create")
	c.vertexPath = make([]vertexSelect, 0, 10)
	c.edgePath = make([]edgeSelect, 0, 10)
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
	c.curVariable = ""
	c.curLabels = []string{}
}

func (c *cypherListener) ExitOC_RelationshipPattern(ctx *parser.OC_RelationshipPatternContext) {
	e := edgeSelect{name: c.curVariable, label: c.curLabels}
	log.Debugf("RelationshipPattern: %s", e)
	c.edgePath = append(c.edgePath, e)
}

func (c *cypherListener) EnterOC_LabelName(ctx *parser.OC_LabelNameContext) {
	log.Debugf("Label: %s", ctx.GetText())
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
