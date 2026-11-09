package gripper

import (
	"fmt"

	"github.com/bmeg/grip/gdbi"
	"google.golang.org/protobuf/types/known/structpb"
)

// ElementToProto converts a data element to its wire form. A nil element gives nil.
func ElementToProto(e *gdbi.DataElement) (*Element, error) {
	if e == nil {
		return nil, nil
	}
	out := &Element{Id: e.ID, Label: e.Label, From: e.From, To: e.To, Loaded: e.Loaded}
	if e.Loaded && e.Data != nil {
		s, err := structpb.NewStruct(e.Data)
		if err != nil {
			return nil, fmt.Errorf("element %s: %w", e.ID, err)
		}
		out.Data = s
	}
	return out, nil
}

// ProtoToElement converts a wire element to a data element. A nil element gives nil.
func ProtoToElement(e *Element) *gdbi.DataElement {
	if e == nil {
		return nil
	}
	out := &gdbi.DataElement{ID: e.Id, Label: e.Label, From: e.From, To: e.To, Loaded: e.Loaded}
	if e.Data != nil {
		out.Data = e.Data.AsMap()
	} else if e.Loaded {
		out.Data = map[string]interface{}{}
	}
	return out
}

// PathToProto converts traveler path entries to the wire form.
func PathToProto(p []gdbi.DataElementID) []*PathElem {
	out := make([]*PathElem, len(p))
	for i, e := range p {
		out[i] = &PathElem{Vertex: e.Vertex, Edge: e.Edge}
	}
	return out
}

// ProtoToPath converts wire path entries to traveler path entries.
func ProtoToPath(p []*PathElem) []gdbi.DataElementID {
	out := make([]gdbi.DataElementID, len(p))
	for i, e := range p {
		out[i] = gdbi.DataElementID{Vertex: e.Vertex, Edge: e.Edge}
	}
	return out
}

// NewTravelerIn builds the wire form of a traveler entering a segment. The traveler's
// full state stays with the caller; only the current element is sent. A nil
// traveler or one without a current element is a seed.
func NewTravelerIn(requestID uint64, cursor uint32, t gdbi.Traveler) (*TravelerIn, error) {
	out := &TravelerIn{RequestId: requestID, Cursor: cursor}
	if t == nil || t.IsNull() {
		return out, nil
	}
	cur, err := ElementToProto(t.GetCurrent().Get())
	if err != nil {
		return nil, err
	}
	out.Current = cur
	return out, nil
}

// TravelerToOut builds the result message for a traveler that ran inside a segment.
// The traveler must have been started with gdbi.NewEntryTraveler (or as a seed), so
// that its marks and path are the delta relative to the sender's copy.
// A null traveler produces a result with no current element and NullMove set.
func TravelerToOut(requestID uint64, cursor uint32, t gdbi.Traveler) (*TravelerOut, error) {
	out := &TravelerOut{RequestId: requestID, Cursor: cursor}
	if t.IsNull() {
		out.NullMove = true
	} else {
		cur, err := ElementToProto(t.GetCurrent().Get())
		if err != nil {
			return nil, err
		}
		out.Current = cur
	}
	if names := t.ListMarks(); len(names) > 0 {
		out.Marks = make(map[string]*Element, len(names))
		for _, n := range names {
			m, err := ElementToProto(t.GetMark(n).Get())
			if err != nil {
				return nil, err
			}
			out.Marks[n] = m
		}
	}
	if p := t.GetPath(); len(p) > 0 {
		out.Path = PathToProto(p)
	}
	return out, nil
}

// ApplyTravelerOut returns the traveler that results from applying a segment
// result to the traveler that was sent (orig). orig is not modified, so it can
// be reused for the other results of the same request.
func ApplyTravelerOut(orig *gdbi.BaseTraveler, out *TravelerOut) *gdbi.BaseTraveler {
	var cur gdbi.DataRef
	if !out.NullMove && out.Current != nil {
		cur = ProtoToElement(out.Current)
	}
	var marks map[string]*gdbi.DataElement
	if len(out.Marks) > 0 {
		marks = make(map[string]*gdbi.DataElement, len(out.Marks))
		for k, v := range out.Marks {
			marks[k] = ProtoToElement(v)
		}
	}
	return orig.ApplyDelta(cur, marks, ProtoToPath(out.Path))
}
