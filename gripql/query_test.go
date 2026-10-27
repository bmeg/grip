package gripql

import (
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestSameAsBuilderSerialization(t *testing.T) {
	statement := NewQuery().SameAs("node").Statements[0]
	if _, ok := statement.GetStatement().(*GraphStatement_SameAs); !ok {
		t.Fatalf("SameAs builder created unexpected statement %T", statement.GetStatement())
	}

	wire, err := proto.Marshal(statement)
	if err != nil {
		t.Fatalf("failed to marshal SameAs statement: %v", err)
	}
	decoded := &GraphStatement{}
	if err := proto.Unmarshal(wire, decoded); err != nil {
		t.Fatalf("failed to unmarshal SameAs statement: %v", err)
	}
	if !proto.Equal(statement, decoded) {
		t.Fatalf("SameAs statement changed during binary serialization: %v", decoded)
	}

	json, err := protojson.Marshal(statement)
	if err != nil {
		t.Fatalf("failed to marshal SameAs statement as JSON: %v", err)
	}
	if string(json) != `{"sameAs":"node"}` {
		t.Fatalf("unexpected SameAs JSON: %s", json)
	}
}
