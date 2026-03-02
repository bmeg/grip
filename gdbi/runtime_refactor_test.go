package gdbi

import (
	"reflect"
	"testing"
)

func TestDataElementCopyImmutableUsesCopyOnWrite(t *testing.T) {
	orig := &DataElement{
		ID:      "v1",
		Label:   "L",
		Data:    map[string]any{"a": 1},
		Loaded:  true,
		Mutable: false,
	}
	cp, ok := orig.Copy().(*DataElement)
	if !ok {
		t.Fatalf("copy type mismatch: %T", orig.Copy())
	}

	if reflect.ValueOf(orig.Data).Pointer() != reflect.ValueOf(cp.Data).Pointer() {
		t.Fatalf("immutable copy should share payload map before mutation")
	}

	cp.EnsureMutablePayload()
	cp.Data["a"] = 2

	if got := orig.Data["a"]; got != 1 {
		t.Fatalf("copy-on-write failed, original mutated: %v", got)
	}
	if got := cp.Data["a"]; got != 2 {
		t.Fatalf("copy mutation failed: %v", got)
	}
}

func TestDataElementCopyMutableDeepCopies(t *testing.T) {
	orig := &DataElement{
		ID:      "v1",
		Label:   "L",
		Data:    map[string]any{"a": 1},
		Loaded:  true,
		Mutable: true,
	}
	cp, ok := orig.Copy().(*DataElement)
	if !ok {
		t.Fatalf("copy type mismatch: %T", orig.Copy())
	}

	if reflect.ValueOf(orig.Data).Pointer() == reflect.ValueOf(cp.Data).Pointer() {
		t.Fatalf("mutable copy should deep copy payload map")
	}

	cp.Data["a"] = 2
	if got := orig.Data["a"]; got != 1 {
		t.Fatalf("original mutated by mutable copy: %v", got)
	}
}

func TestAddCurrentPathTrackingDisabledByDefault(t *testing.T) {
	var tr Traveler = &BaseTraveler{}
	tr = tr.AddCurrent(&DataElement{ID: "v1", Label: "L"})
	if p := tr.GetPath(); len(p) != 0 {
		t.Fatalf("expected no path tracking by default, got %d entries", len(p))
	}
}

func TestAddCurrentPathTrackingEnabled(t *testing.T) {
	var tr Traveler = &BaseTraveler{TrackPath: true}
	tr = tr.AddCurrent(&DataElement{ID: "v1", Label: "L"})
	tr = tr.AddCurrent(&DataElement{ID: "e1", Label: "E", To: "v2"})
	path := tr.GetPath()
	if len(path) != 2 {
		t.Fatalf("expected 2 path entries, got %d", len(path))
	}
	if path[0].Vertex != "v1" {
		t.Fatalf("unexpected first path entry: %#v", path[0])
	}
	if path[1].Edge != "e1" {
		t.Fatalf("unexpected second path entry: %#v", path[1])
	}
}

func TestElementLookupMetaCanonicalAccess(t *testing.T) {
	v := ElementLookup{Meta: LookupMeta{UID: 7}}
	meta, ok := v.GetLookupMeta()
	if !ok || meta.UID != 7 {
		t.Fatalf("value meta access failed: ok=%v meta=%+v", ok, meta)
	}

	p := ElementLookup{Meta: LookupMeta{UID: 9}}
	meta, ok = p.GetLookupMeta()
	if !ok || meta.UID != 9 {
		t.Fatalf("pointer meta access failed: ok=%v meta=%+v", ok, meta)
	}
}

func TestDataElementModeInference(t *testing.T) {
	raw := &DataElement{RawJSON: `{"a":1}`}
	if raw.Mode() != RowModeRaw {
		t.Fatalf("expected raw mode, got %v", raw.Mode())
	}

	ref := &DataElement{Data: map[string]any{}, Loaded: false}
	if ref.Mode() != RowModeReference {
		t.Fatalf("expected reference mode, got %v", ref.Mode())
	}

	proj := &DataElement{Data: map[string]any{"a": 1}, Loaded: false}
	if proj.Mode() != RowModeProjected {
		t.Fatalf("expected projected mode, got %v", proj.Mode())
	}

	mat := &DataElement{Data: map[string]any{"a": 1}, Loaded: true}
	if mat.Mode() != RowModeMaterialized {
		t.Fatalf("expected materialized mode, got %v", mat.Mode())
	}
}

func TestTravelerAddCurrentSharesImmutableRow(t *testing.T) {
	row := &DataElement{
		ID:      "v1",
		Label:   "L",
		Data:    map[string]any{"a": 1},
		Loaded:  true,
		Mutable: false,
	}
	base := &BaseTraveler{}
	next, ok := base.AddCurrent(row).(*BaseTraveler)
	if !ok {
		t.Fatalf("unexpected traveler type: %T", base.AddCurrent(row))
	}
	got, ok := next.Current.(*DataElement)
	if !ok {
		t.Fatalf("unexpected current row type: %T", next.Current)
	}
	if got != row {
		t.Fatalf("expected immutable row pointer to be shared")
	}
}

func TestTravelerAddCurrentCopiesMutableRow(t *testing.T) {
	row := &DataElement{
		ID:      "v1",
		Label:   "L",
		Data:    map[string]any{"a": 1},
		Loaded:  true,
		Mutable: true,
	}
	base := &BaseTraveler{}
	next, ok := base.AddCurrent(row).(*BaseTraveler)
	if !ok {
		t.Fatalf("unexpected traveler type: %T", base.AddCurrent(row))
	}
	got, ok := next.Current.(*DataElement)
	if !ok {
		t.Fatalf("unexpected current row type: %T", next.Current)
	}
	if got == row {
		t.Fatalf("expected mutable row pointer to be copied")
	}
	if reflect.ValueOf(got.Data).Pointer() == reflect.ValueOf(row.Data).Pointer() {
		t.Fatalf("expected mutable row payload map to be deep-copied")
	}
}
