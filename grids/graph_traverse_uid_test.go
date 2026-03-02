package grids

import (
	"testing"

	"github.com/bmeg/grip/gdbi"
)

func TestLookupUIDFromRequestPrivValue(t *testing.T) {
	req := gdbi.ElementLookup{
		Meta: gdbi.LookupMeta{UID: 123},
	}
	uid, ok := lookupUIDFromRequest(req, nil)
	if !ok {
		t.Fatalf("expected uid lookup to succeed from value priv")
	}
	if uid != 123 {
		t.Fatalf("unexpected uid: got %d want %d", uid, 123)
	}
}

func TestLookupUIDFromRequestPrivPointer(t *testing.T) {
	req := gdbi.ElementLookup{
		Meta: gdbi.LookupMeta{UID: 456},
	}
	uid, ok := lookupUIDFromRequest(req, nil)
	if !ok {
		t.Fatalf("expected uid lookup to succeed from pointer priv")
	}
	if uid != 456 {
		t.Fatalf("unexpected uid: got %d want %d", uid, 456)
	}
}

func TestLookupUIDFromRequestMissing(t *testing.T) {
	req := gdbi.ElementLookup{}
	if _, ok := lookupUIDFromRequest(req, nil); ok {
		t.Fatalf("expected uid lookup to fail when no priv/id are available")
	}
}
