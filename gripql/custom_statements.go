
package gripql

//These are custom graph custom statements, which represent operations
//in the traversal that the optimizer may add in, but can't be coded by a
//serialized user request

type GraphStatement_LookupVertsIndex struct {
  Labels []string
}

func (*GraphStatement_LookupVertsIndex) isGraphStatement_Statement()         {}

type GraphStatement_EngineCustom struct {
  Desc string
  Custom interface{}
}

func (*GraphStatement_EngineCustom) isGraphStatement_Statement()         {}
