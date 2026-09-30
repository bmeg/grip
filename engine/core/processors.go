package core

import (
	"bytes"
	"context"
	"fmt"

	"github.com/bmeg/grip/engine/logic"
	"github.com/bmeg/grip/gdbi"

	//"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/copy"
	"github.com/spf13/cast"
)

////////////////////////////////////////////////////////////////////////////////

// LookupVerts starts query by looking on vertices
type LookupVerts struct {
	db       gdbi.GraphInterface
	ids      []string
	loadData bool
}

// Process LookupVerts
func (l *LookupVerts) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			if len(l.ids) == 0 {
				for v := range l.db.GetVertexList(ctx, l.loadData) {
					out <- t.AddCurrent(&gdbi.DataElement{
						ID:     v.ID,
						Label:  v.Label,
						Data:   v.Data,
						Loaded: l.loadData,
					})
				}
			} else {
				for _, i := range l.ids {
					v := l.db.GetVertex(i, l.loadData)
					if v != nil {
						out <- t.AddCurrent(&gdbi.DataElement{
							ID:     v.ID,
							Label:  v.Label,
							Data:   v.Data,
							Loaded: l.loadData,
						})
					}
				}
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// LookupVertsIndex look up vertices by indexed based feature
type LookupVertsLabelIndex struct {
	db       gdbi.GraphInterface
	labels   []string
	loadData bool
}

// Process LookupVertsIndex
func (l *LookupVertsLabelIndex) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	queryChan := make(chan gdbi.ElementLookup, 100)
	go func() {
		defer close(queryChan)
		for t := range in {
			for _, label := range l.labels {
				for id := range l.db.VertexLabelScan(ctx, label) {
					queryChan <- gdbi.ElementLookup{
						ID:  id,
						Ref: t,
					}
				}
			}
		}
	}()

	go func() {
		defer close(out)
		for v := range l.db.GetVertexChannel(ctx, queryChan, l.loadData) {
			i := v.Ref
			out <- i.AddCurrent(v.Vertex)
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Fields selects fields from current element
type Fields struct {
	keys []string
}

// Process runs Values step
func (f *Fields) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			o := gdbi.SelectTravelerFields(t, f.keys...)
			out <- o
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Render takes current state and renders into requested structure
type Render struct {
	Template any
}

// Process runs the render processor
func (r *Render) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			v := gdbi.RenderTraveler(t, r.Template)
			out <- &gdbi.BaseTraveler{Render: v}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Path tells system to return path data
type Path struct {
	Template any //this isn't really used yet.
}

// Process runs the render processor
func (r *Path) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			out <- t
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Unwind takes an array field and replicates the message for every element in the array
type Unwind struct {
	Field string
}

// Process runs the render processor
func (r *Unwind) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			v := gdbi.TravelerPathLookup(t, r.Field)
			//log.Debugln("UNWIND V RES: ", v)
			if a, ok := v.([]any); ok {
				cur := t.GetCurrent()
				if len(a) > 0 {
					for _, i := range a {
						o := gdbi.DataElement{
							ID:    cur.Get().ID,
							Label: cur.Get().Label,
							From:  cur.Get().From,
							To:    cur.Get().To,
							Data:  copy.DeepCopy(cur.Get().Data).(map[string]any), Loaded: true,
						}
						n := t.AddCurrent(&o)
						gdbi.TravelerSetValue(n, r.Field, i)
						out <- n
					}
				} else {
					o := gdbi.DataElement{ID: cur.Get().ID, Label: cur.Get().Label, From: cur.Get().From, To: cur.Get().To, Data: copy.DeepCopy(cur.Get().Data).(map[string]interface{}), Loaded: true}
					n := t.AddCurrent(&o)
					gdbi.TravelerSetValue(n, r.Field, nil)
					out <- n
				}
			} else {
				cur := t.GetCurrent()
				// if outnull returns null cur can be empty
				if cur.Get() != nil {
					o := gdbi.DataElement{
						ID:    cur.Get().ID,
						Label: cur.Get().Label,
						From:  cur.Get().From,
						To:    cur.Get().To,
						Data:  copy.DeepCopy(cur.Get().Data).(map[string]any), Loaded: true,
					}
					n := t.AddCurrent(&o)
					gdbi.TravelerSetValue(n, r.Field, nil)
					out <- n
				}
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// ToType
type ToType struct {
	Field    string
	TypeName string
}

func (tt *ToType) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}

			totype := logic.ConvertToType(gdbi.TravelerPathLookup(t, tt.Field), tt.TypeName)
			gdbi.TravelerSetValue(t, tt.Field, totype)
			out <- t

		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Count incoming elements
type Count struct{}

// Process runs Count
func (c *Count) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		var i uint32
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			i++
		}
		out <- &gdbi.BaseTraveler{Count: i}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Limit limits incoming values to count
type Limit struct {
	count uint32
}

// Process runs limit
func (l *Limit) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	newCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(out)
		var i uint32
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			if i < l.count {
				out <- t
			} else if i == l.count {
				cancel()
			}
			i++
		}
	}()
	return newCtx
}

////////////////////////////////////////////////////////////////////////////////

// Skip limits incoming values to count
type Skip struct {
	count uint32
}

// Process runs offset
func (o *Skip) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		var i uint32
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			if i >= o.count {
				out <- t
			}
			i++
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Range limits the number of travelers returned.
// When the low-end of the range is not met, objects are continued to be iterated.
// When within the low (inclusive) and high (exclusive) range, traversers are emitted.
// When above the high range, the traversal breaks out of iteration. Finally, the use of -1 on the high range will emit remaining traversers after the low range begins.
type Range struct {
	start int32
	stop  int32
}

// Process runs range
func (r *Range) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	newCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(out)
		var i int32
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			if i >= r.start && (i < r.stop || r.stop == -1) {
				out <- t
			} else if i == r.stop {
				cancel()
			}
			i++
		}
	}()
	return newCtx
}

////////////////////////////////////////////////////////////////////////////////

// Distinct only returns unique objects as defined by the set of select features
type Distinct struct {
	vals []string
}

// Process runs distinct
func (g *Distinct) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		kv := man.GetTempKV()
		defer kv.Close()
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			s := make([][]byte, len(g.vals))
			found := true
			for i, v := range g.vals {
				if gdbi.TravelerPathExists(t, v) {
					s[i] = []byte(fmt.Sprintf("%#v", gdbi.TravelerPathLookup(t, v)))
				} else {
					found = false
				}
			}
			k := bytes.Join(s, []byte{0x00})
			if found && len(k) > 0 {
				if !kv.HasKey(k) {
					kv.Set(k, []byte{0x01})
					out <- t
				}
			}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Marker marks the current element
type Marker struct {
	mark string
}

// Process runs Marker
func (m *Marker) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			out <- t.AddMark(m.mark, t.GetCurrent())
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// Selector selects marks to return
type Selector struct {
	marks []string
}

// Process runs Selector
func (s *Selector) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			res := map[string]*gdbi.DataElement{}
			for _, mark := range s.marks {
				val := t.GetMark(mark)
				if val == nil {
					val = &gdbi.DataElement{}
				}
				res[mark] = val.Get()
			}
			out <- &gdbi.BaseTraveler{Selections: res}
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

type ValueSet struct {
	key   string
	value any
}

func (s *ValueSet) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			gdbi.TravelerSetValue(t, s.key, s.value)
			out <- t
		}
	}()
	return ctx
}

type ValueIncrement struct {
	key   string
	value int32
}

func (s *ValueIncrement) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			v := gdbi.TravelerPathLookup(t, s.key)
			i := cast.ToInt(v) + int(s.value)
			o := t.Copy()
			gdbi.TravelerSetValue(o, s.key, i)
			out <- o
		}
	}()
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

// MarkSelect moves to selected mark
type MarkSelect struct {
	mark string
}

// Process runs Selector
func (s *MarkSelect) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	go func() {
		defer close(out)
		for t := range in {
			if t.IsSignal() {
				out <- t
				continue
			}
			m := t.GetMark(s.mark)
			n := t.AddCurrent(m)
			// Select should count as a path step even when selecting the same element.
			if len(n.GetPath()) == len(t.GetPath()) {
				if bt, ok := n.(*gdbi.BaseTraveler); ok {
					de := m.Get()
					if de == nil {
						bt.Path = append(bt.Path, gdbi.DataElementID{})
					} else if de.To != "" {
						bt.Path = append(bt.Path, gdbi.DataElementID{Edge: de.ID})
					} else {
						bt.Path = append(bt.Path, gdbi.DataElementID{Vertex: de.ID})
					}
				}
			}
			out <- n
		}
	}()
	return ctx
}
