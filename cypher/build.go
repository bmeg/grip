
package cypher


import (
	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/bmeg/grip/cypher/parser"

  "github.com/bmeg/grip/gripql"
  log "github.com/sirupsen/logrus"

)

type vertexSelect struct {
  name  string
  label []string
}

type edgeSelect struct {
  name  string
  label []string
}

type cypherListener struct {
  *parser.BaseCypherListener
  vertexPath []vertexSelect
  edgePath []edgeSelect
  returns []string

  curVariable string
  curLabels []string
}

func (c *cypherListener) BuildQuery() *gripql.Query {
  q := gripql.NewQuery()
  q = q.V()
  if len(c.vertexPath) > 0 && len(c.vertexPath[0].label) > 0 {
    q = q.HasLabel(c.vertexPath[0].label[0])
  }
  for i := range c.vertexPath {
    q = q.As(c.vertexPath[i].name)
  }
  if len(c.returns) > 0 {
    q = q.Select(c.returns...)
  }
  log.Printf("Query: %s\n", q.String())
  return q
}

func (c *cypherListener) EnterOC_Statement(ctx *parser.OC_StatementContext) {
  log.Printf("Entering Statement %#v\n", ctx.GetText())
}

func (c *cypherListener) EnterOC_Match(ctx *parser.OC_MatchContext) {
  log.Printf("Is Match\n")
  c.vertexPath = make([]vertexSelect, 0, 10)
  c.edgePath = make([]edgeSelect, 0, 10)
}

func (c *cypherListener) ExitOC_Match(ctx *parser.OC_MatchContext) {
  log.Printf("Building Query: %#v\n", c.vertexPath)
}


func (c *cypherListener) EnterOC_PatternElement(ctx *parser.OC_PatternElementContext) {
  log.Printf("Is pattern %s\n", ctx.GetText())
}

func (c *cypherListener) EnterOC_NodePattern(ctx *parser.OC_NodePatternContext) {
  log.Printf("NodePattern: %s\n", ctx.GetText())
  c.curVariable = ""
  c.curLabels = []string{}
}

func (c *cypherListener) ExitOC_NodePattern(ctx *parser.OC_NodePatternContext) {
  c.vertexPath = append(c.vertexPath, vertexSelect{name:c.curVariable, label:c.curLabels})
}

func (c *cypherListener) EnterOC_Variable(ctx *parser.OC_VariableContext) {
  log.Printf("Variable: %s\n", ctx.GetText())
  c.curVariable = ctx.GetText()
}

func (c *cypherListener) EnterOC_RelationshipPattern(ctx *parser.OC_RelationshipPatternContext) {
  log.Printf("RelationshipPattern: %s\n", ctx.GetText())
  c.curVariable = ""
  c.curLabels = []string{}
}


func (c *cypherListener) ExitOC_RelationshipPattern(ctx *parser.OC_RelationshipPatternContext) {
  e := edgeSelect{name:c.curVariable, label:c.curLabels}
  log.Printf("RelationshipPattern: %s\n", e)
  c.edgePath = append(c.edgePath, e)
}

func (c *cypherListener) EnterOC_LabelName(ctx *parser.OC_LabelNameContext) {
  log.Printf("Label: %s\n", ctx.GetText())
  c.curLabels = append(c.curLabels, ctx.GetText())
}

func (c *cypherListener) EnterOC_Return(ctx *parser.OC_ReturnContext) {
  c.returns = []string{}
}


func (c *cypherListener) EnterOC_ReturnItem(ctx *parser.OC_ReturnItemContext) {
  log.Printf("Return: %s\n", ctx.GetText())
  c.returns = append(c.returns, ctx.GetText())
}

func RunParser(oc string) *gripql.Query {
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
