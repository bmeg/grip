package mongo

import (
	//"fmt"
	"github.com/bmeg/arachne/aql"
	"github.com/bmeg/arachne/gdbi"
	//"github.com/bmeg/arachne/protoutil"
	"github.com/bmeg/arachne/engine/core"
	//"log"
)

type MongoCompiler struct {
	db gdbi.GraphInterface
	compiler gdbi.Compiler
}

func NewCompiler(db *Graph) gdbi.Compiler {
	return &MongoCompiler{db: db, compiler:core.NewCompiler(db)}
}


func (comp *MongoCompiler) Compile(stmts []*aql.GraphStatement, workDir string) (gdbi.Pipeline, error) {
	return comp.compiler.Compile(stmts, workDir)
}
