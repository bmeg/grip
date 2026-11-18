package gripper

import (
	"reflect"
	"testing"

	"github.com/bmeg/grip/gdbi"
)

func TestElementRoundTrip(t *testing.T) {
	in := &gdbi.DataElement{ID: "A:1-x-B:2", Label: "x", From: "A:1", To: "B:2",
		Data: map[string]interface{}{"n": float64(3), "s": "v"}, Loaded: true}
	p, err := ElementToProto(in)
	if err != nil {
		t.Fatal(err)
	}
	out := ProtoToElement(p)
	if !reflect.DeepEqual(in, out) {
		t.Errorf("round trip mismatch: %#v != %#v", in, out)
	}
	bare := ProtoToElement(&Element{Id: "B:9"})
	if bare.Loaded || bare.ID != "B:9" {
		t.Errorf("bare reference: %#v", bare)
	}
	if e, _ := ElementToProto(nil); e != nil {
		t.Error("nil element must stay nil")
	}
}

func TestSegmentDeltaMatchesLocalTraversal(t *testing.T) {
	v1 := &gdbi.DataElement{ID: "A:1", Label: "A", Loaded: true}
	e1 := &gdbi.DataElement{ID: "A:1-l-B:1", Label: "l", From: "A:1", To: "B:1", Loaded: true}
	v2 := &gdbi.DataElement{ID: "B:1", Label: "B", Loaded: true}
	v3 := &gdbi.DataElement{ID: "B:2", Label: "B", Loaded: true}

	// monolithic traversal: seed, mark, move, edge, move
	local := (&gdbi.BaseTraveler{}).AddCurrent(v1).AddMark("a", v1)
	want := local.AddCurrent(e1).AddCurrent(v2).AddMark("b", v2).AddCurrent(v3)

	// central keeps the traveler after the seed; the node continues from v1
	central := (&gdbi.BaseTraveler{}).AddCurrent(v1).(*gdbi.BaseTraveler)
	central = central.AddMark("a", v1).(*gdbi.BaseTraveler)

	in, err := NewTravelerIn(7, 1, central)
	if err != nil {
		t.Fatal(err)
	}
	node := gdbi.NewEntryTraveler(ProtoToElement(in.Current))
	res := node.AddCurrent(e1).AddCurrent(v2).AddMark("b", v2).AddCurrent(v3)

	out, err := TravelerToOut(in.RequestId, 5, res)
	if err != nil {
		t.Fatal(err)
	}
	if out.RequestId != 7 || out.Cursor != 5 {
		t.Fatalf("bad header %v", out)
	}
	got := ApplyTravelerOut(central, out)

	if !reflect.DeepEqual(want.GetPath(), got.GetPath()) {
		t.Errorf("path: want %v got %v", want.GetPath(), got.GetPath())
	}
	if got.GetCurrentID() != "B:2" {
		t.Errorf("current %s", got.GetCurrentID())
	}
	if !got.HasMark("a") || !got.HasMark("b") {
		t.Errorf("marks %v", got.ListMarks())
	}
	if central.HasMark("b") || len(central.GetPath()) != 1 {
		t.Error("ApplyTravelerOut modified the original traveler")
	}
}

func TestSeedAndNull(t *testing.T) {
	in, err := NewTravelerIn(1, 0, nil)
	if err != nil || in.Current != nil {
		t.Fatalf("seed: %v %v", in, err)
	}
	out, err := TravelerToOut(1, 2, &gdbi.BaseTraveler{})
	if err != nil || !out.NullMove || out.Current != nil {
		t.Fatalf("null: %v %v", out, err)
	}
	orig := (&gdbi.BaseTraveler{}).AddCurrent(&gdbi.DataElement{ID: "A:1", Loaded: true}).(*gdbi.BaseTraveler)
	got := ApplyTravelerOut(orig, out)
	if !got.IsNull() {
		t.Error("expected null traveler")
	}
}
