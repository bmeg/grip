package main

import (
	"fmt"
	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/bmeg/cypher/parser"
)


type cypherListener struct {
  *parser.BaseCypherListener
}


func (c *cypherListener) EnterOC_Statement(ctx *parser.OC_StatementContext) {
  fmt.Printf("Entering Statement %#v\n", ctx.GetText())
}

func (c *cypherListener) EnterOC_Match(ctx *parser.OC_MatchContext) {
  fmt.Printf("Is Match\n")
}

func (c *cypherListener) EnterOC_PatternElement(ctx *parser.OC_PatternElementContext) {
  fmt.Printf("Is pattern %s\n", ctx.GetText())
}

func (c *cypherListener) EnterOC_NodePattern(ctx *parser.OC_NodePatternContext) {
  fmt.Printf("NodePattern: %s\n", ctx.GetText())
}

func (c *cypherListener) EnterOC_RelationshipPattern(ctx *parser.OC_RelationshipPatternContext) {
  fmt.Printf("RelationshipPattern: %s\n", ctx.GetText())  
}


func RunParser(oc string) {
  // Setup the input
	is := antlr.NewInputStream(oc)

	// Create the Lexer
	lexer := parser.NewCypherLexer(is)

	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// Create the Parser
	p := parser.NewCypherParser(stream)

	// Finally parse the expression
	antlr.ParseTreeWalkerDefault.Walk(&cypherListener{}, p.OC_Cypher())

}

func main() {
	//t1 := "MATCH (p:Person)-[:LIKES]->(t:Technology) RETURN p"
  //RunParser(t1)
  //fmt.Printf("Done\n")

  t2 := "MATCH p = (a {name: 'A'})-[rel1]->(b)-[rel2]->(c) RETURN p"
  RunParser(t2)
}
