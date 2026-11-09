package gripper

import (
	"context"
	"fmt"
	"testing"

	"github.com/bmeg/grip/gripql"
	"google.golang.org/grpc"
)

type fakeSource struct {
	info *SourceInfo
}

func (f *fakeSource) GetInfo(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*SourceInfo, error) {
	return f.info, nil
}

func (f *fakeSource) Traverse(ctx context.Context, opts ...grpc.CallOption) (GRIPSource_TraverseClient, error) {
	return nil, fmt.Errorf("not implemented")
}

func fake(prefix string, labels []string, links ...*EdgeLink) *fakeSource {
	g := &gripql.Graph{}
	for _, l := range labels {
		g.Vertices = append(g.Vertices, &gripql.Vertex{Id: l, Label: l})
	}
	return &fakeSource{&SourceInfo{ProtocolVersion: ProtocolVersion, Name: prefix, Prefix: prefix, Schema: g, EdgeLinks: links}}
}

func TestRegistryPrefixValidation(t *testing.T) {
	r := NewRegistry()
	if err := r.Add("a", fake("Character:", nil)); err != nil {
		t.Fatal(err)
	}
	if err := r.Add("dup", fake("Character:", nil)); err == nil {
		t.Error("equal prefix accepted")
	}
	if err := r.Add("sub", fake("Character:x:", nil)); err == nil {
		t.Error("overlapping prefix accepted")
	}
	if err := r.Add("empty", fake("", nil)); err == nil {
		t.Error("empty prefix accepted")
	}
	bad := fake("Other:", nil)
	bad.info.ProtocolVersion = 1
	if err := r.Add("old", bad); err == nil {
		t.Error("wrong protocol version accepted")
	}
	if err := r.Add("a", fake("Planet:", nil)); err == nil {
		t.Error("duplicate name accepted")
	}
}

func TestRegistryOwner(t *testing.T) {
	r := NewRegistry()
	r.Add("chars", fake("Character:", nil))
	r.Add("planets", fake("Planet:", nil))
	if s := r.Owner("Planet:7"); s == nil || s.Name != "planets" {
		t.Errorf("owner %v", s)
	}
	if s := r.Owner("Planet:7-homeworld-Character:1"); s == nil || s.Name != "planets" {
		t.Errorf("edge owner %v", s)
	}
	if r.Owner("Film:1") != nil {
		t.Error("unexpected owner")
	}
	r.Remove("planets")
	if r.Owner("Planet:7") != nil {
		t.Error("removed source still owns")
	}
}

func TestRegistryInboundSources(t *testing.T) {
	r := NewRegistry()
	r.Add("chars", fake("Character:", []string{"Character"},
		&EdgeLink{Label: "homeworld", FromPrefix: "Character:", ToPrefix: "Planet:"}))
	r.Add("planets", fake("Planet:", []string{"Planet"}))
	r.Add("films", fake("Film:", []string{"Film"},
		&EdgeLink{Label: "characters", FromPrefix: "Film:", ToPrefix: "Character:"}))

	got := r.InboundSources("Planet:", nil)
	if len(got) != 1 || got[0].Name != "chars" {
		t.Errorf("planet inbound: %v", got)
	}
	if got := r.InboundSources("Planet:", []string{"residents"}); len(got) != 0 {
		t.Errorf("label filter: %v", got)
	}
	got = r.InboundSources("Character:", []string{"characters"})
	if len(got) != 1 || got[0].Name != "films" {
		t.Errorf("character inbound: %v", got)
	}
	// edges leaving Character: that are stored in another source count as well
	if got := r.InboundSources("Film:", nil); len(got) != 0 {
		t.Errorf("film inbound (owner excluded): %v", got)
	}
}

func TestRegistrySourcesFor(t *testing.T) {
	r := NewRegistry()
	r.Add("chars", fake("Character:", []string{"Character"}))
	r.Add("planets", fake("Planet:", []string{"Planet"}))
	r.Add("unknown", fake("Misc:", nil))

	names := func(s []*Source) string {
		o := ""
		for _, i := range s {
			o += i.Name + ","
		}
		return o
	}
	if n := names(r.SourcesFor(nil, nil)); n != "chars,planets,unknown," {
		t.Errorf("all: %s", n)
	}
	if n := names(r.SourcesFor([]string{"Planet"}, nil)); n != "planets,unknown," {
		t.Errorf("labels: %s", n)
	}
	if n := names(r.SourcesFor(nil, []string{"Character:1", "Misc:4"})); n != "chars,unknown," {
		t.Errorf("ids: %s", n)
	}
	if n := names(r.SourcesFor([]string{"Planet"}, []string{"Character:1"})); n != "" {
		t.Errorf("ids+labels: %s", n)
	}
}
