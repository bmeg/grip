
package gripql

//These are custom graph custom statements, which represent operations
//in the traversal that the optimizer may add in, but can't be coded by a
//serialized user request

type GraphStatement_LookupVertsIndex struct {
  Labels []string   `protobuf:"bytes,1,rep,name=labels" json:"labels,omitempty"`
}

func (*GraphStatement_LookupVertsIndex) isGraphStatement_Statement()         {}

type GraphStatement_EngineCustom struct {
  Desc string        `protobuf:"bytes,1,opt,name=desc" json:"desc,omitempty"`
  Custom interface{} `protobuf:"bytes,2,opt,name=custom" json:"custom,omitempty"`
}

func (*GraphStatement_EngineCustom) isGraphStatement_Statement()         {}