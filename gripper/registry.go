package gripper

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bmeg/grip/log"
)

// ProtocolVersion is the GRIPSource protocol version implemented by this package.
const ProtocolVersion = 2

const getInfoTimeout = 30 * time.Second

// Source is a registered resource node.
type Source struct {
	Name   string
	Prefix string
	Client GRIPSourceClient
	Info   *SourceInfo
}

// Registry tracks the resource nodes that make up a federation and answers
// routing questions about them. Prefixes of registered sources never overlap.
type Registry struct {
	mutex   sync.RWMutex
	sources map[string]*Source
}

func NewRegistry() *Registry {
	return &Registry{sources: map[string]*Source{}}
}

// Add connects a source by calling GetInfo and registers it under name. It
// fails if the prefix is empty or overlaps the prefix of another source.
func (r *Registry) Add(name string, client GRIPSourceClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), getInfoTimeout)
	defer cancel()
	info, err := client.GetInfo(ctx, &Empty{})
	if err != nil {
		return fmt.Errorf("source %s: GetInfo failed: %w", name, err)
	}
	return r.AddWithInfo(name, client, info)
}

// AddWithInfo registers a source whose SourceInfo is already known.
func (r *Registry) AddWithInfo(name string, client GRIPSourceClient, info *SourceInfo) error {
	if info.GetPrefix() == "" {
		return fmt.Errorf("source %s: empty prefix", name)
	}
	if v := info.GetProtocolVersion(); v != ProtocolVersion {
		return fmt.Errorf("source %s: protocol version %d, expected %d", name, v, ProtocolVersion)
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if _, ok := r.sources[name]; ok {
		return fmt.Errorf("source %s already registered", name)
	}
	for _, s := range r.sources {
		if strings.HasPrefix(s.Prefix, info.Prefix) || strings.HasPrefix(info.Prefix, s.Prefix) {
			return fmt.Errorf("source %s: prefix %q overlaps prefix %q of source %s", name, info.Prefix, s.Prefix, s.Name)
		}
	}
	r.sources[name] = &Source{Name: name, Prefix: info.Prefix, Client: client, Info: info}
	log.Infof("Registered GRIPSource %s with prefix %s", name, info.Prefix)
	return nil
}

func (r *Registry) Remove(name string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.sources, name)
}

// Source returns the source registered under name, or nil.
func (r *Registry) Source(name string) *Source {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.sources[name]
}

// Sources returns all sources ordered by name.
func (r *Registry) Sources() []*Source {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.sortedLocked()
}

func (r *Registry) sortedLocked() []*Source {
	out := make([]*Source, 0, len(r.sources))
	for _, s := range r.sources {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Owner returns the source whose prefix the ID starts with, or nil.
func (r *Registry) Owner(id string) *Source {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	for _, s := range r.sources {
		if strings.HasPrefix(id, s.Prefix) {
			return s
		}
	}
	return nil
}

// InboundSources returns the sources (other than the owner of prefix) that store
// edges connecting to a vertex under prefix: edge links whose ToPrefix is prefix
// (edges pointing at the vertex, used by in/inE) or whose FromPrefix is prefix
// (edges leaving the vertex but stored elsewhere, which both/bothE also need).
// An empty edgeLabels matches any label.
func (r *Registry) InboundSources(prefix string, edgeLabels []string) []*Source {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	out := []*Source{}
	for _, s := range r.sortedLocked() {
		if s.Prefix == prefix {
			continue
		}
		for _, l := range s.Info.GetEdgeLinks() {
			if l.ToPrefix != prefix && l.FromPrefix != prefix {
				continue
			}
			if len(edgeLabels) == 0 || containsString(edgeLabels, l.Label) {
				out = append(out, s)
				break
			}
		}
	}
	return out
}

// SourcesFor returns the sources that may contain vertices matching a start step.
// ids, if not empty, restricts to the owners of those IDs. labels, if not empty,
// restricts to sources that declare one of those vertex labels. A source that
// declares no vertex labels is never pruned by label.
func (r *Registry) SourcesFor(labels []string, ids []string) []*Source {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	out := []*Source{}
	for _, s := range r.sortedLocked() {
		if len(ids) > 0 {
			found := false
			for _, id := range ids {
				if strings.HasPrefix(id, s.Prefix) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		if len(labels) > 0 {
			declared := s.Info.GetSchema().GetVertices()
			if len(declared) > 0 {
				found := false
				for _, v := range declared {
					if containsString(labels, v.Label) {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}
		}
		out = append(out, s)
	}
	return out
}

func containsString(list []string, s string) bool {
	for _, i := range list {
		if i == s {
			return true
		}
	}
	return false
}
