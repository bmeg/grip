package mongo

import (
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/engine/core"
	"github.com/bmeg/arachne/gdbi"
)

// Compiler implements a custom compiler that uses mongo specific optimization
type Compiler struct {
	db       gdbi.GraphInterface
	compiler gdbi.Compiler
}

// NewCompiler creates a new compiler that uses the provided mongo graph to run queries
func NewCompiler(db *Graph) gdbi.Compiler {
	return &Compiler{db: db, compiler: core.NewCompiler(db)}
}

// Compile turns an aql query into an executable graph query
func (comp *Compiler) Compile(stmts []*aql.GraphStatement) (gdbi.Pipeline, error) {
	return comp.compiler.Compile(stmts)
}
