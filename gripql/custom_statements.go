
package gripql

//These are custom graph custom statements, which represent operations
//in the traversal that the optimizer may add in, but can't be coded by a
//serialized user request

type GraphStatement_LookupVertsIndex struct {
  Labels []string
}

func (*GraphStatement_LookupVertsIndex) isGraphStatement_Statement()         {}
