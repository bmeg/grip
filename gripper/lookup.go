package gripper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

const (
	lookupBatchSize  = 256
	lookupFlushEvery = 5 * time.Millisecond
)

type lookupMode int

const (
	lookupVertices lookupMode = iota // results are vertices
	lookupEdges                      // results are edges
)

// lookupSpec describes a family of single-step lookups run by lookupCall.
type lookupSpec struct {
	steps    []*gripql.GraphStatement // [V(), <move>]; V is a placeholder, travelers enter at cursor 1
	mode     lookupMode
	labels   []string
	fanout   bool // also ask sources that hold edges pointing at the vertex (in/inE)
	resolve  bool // load the vertex itself instead of moving
	emitNull bool
}

// lookupStream is a main or resolver stream of a lookupCall.
type lookupStream struct {
	*BatchStream
	resolve     bool
	outstanding int // flushes not yet acked; guarded by lookupCall.mu
}

type pendingLookup struct {
	lk        gdbi.ElementLookup
	remaining int
	found     bool
}

// lookupCall serves one Get*Channel call: a stream of ElementLookup requests turned
// into batched single-step traversals on the sources, with vertices owned by
// other sources resolved by their owners.
type lookupCall struct {
	g      *FederatedGraph
	ctx    context.Context
	cancel context.CancelFunc
	spec   lookupSpec
	out    chan gdbi.ElementLookup

	mu        sync.Mutex
	cond      *sync.Cond
	nextID    uint64
	pending   map[uint64]*pendingLookup
	resolving map[uint64]gdbi.ElementLookup
	streams   map[string]*lookupStream
	resolvers map[string]*lookupStream
	err       error
	consumers sync.WaitGroup
}

func (g *FederatedGraph) lookup(ctx context.Context, req chan gdbi.ElementLookup, spec lookupSpec) chan gdbi.ElementLookup {
	ctx, cancel := context.WithCancel(ctx)
	c := &lookupCall{
		g: g, ctx: ctx, cancel: cancel, spec: spec,
		out:       make(chan gdbi.ElementLookup, 100),
		pending:   map[uint64]*pendingLookup{},
		resolving: map[uint64]gdbi.ElementLookup{},
		streams:   map[string]*lookupStream{},
		resolvers: map[string]*lookupStream{},
	}
	c.cond = sync.NewCond(&c.mu)
	go func() {
		// wake anything waiting for acks when the call is cancelled from outside
		<-ctx.Done()
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	}()
	go c.run(req)
	return c.out
}

func (c *lookupCall) fail(err error) {
	c.mu.Lock()
	if c.err == nil && c.ctx.Err() == nil {
		c.err = err
		log.Errorf("federated lookup failed: %s", err)
	}
	c.mu.Unlock()
	c.cancel()
	c.cond.Broadcast()
}

func (c *lookupCall) emit(l gdbi.ElementLookup) {
	select {
	case c.out <- l:
	case <-c.ctx.Done():
	}
}

func (c *lookupCall) run(req chan gdbi.ElementLookup) {
	defer close(c.out)
	defer c.cancel()

	ticker := time.NewTicker(lookupFlushEvery)
	defer ticker.Stop()
	tickDone := make(chan struct{})
	defer close(tickDone)
	go func() {
		for {
			select {
			case <-ticker.C:
				c.flushAll()
			case <-tickDone:
				return
			case <-c.ctx.Done():
				return
			}
		}
	}()

	for c.ctx.Err() == nil {
		var r gdbi.ElementLookup
		var ok bool
		select {
		case r, ok = <-req:
		default:
			c.flushAll()
			select {
			case r, ok = <-req:
			case <-c.ctx.Done():
				ok = false
			}
		}
		if !ok {
			break
		}
		if r.IsSignal() {
			c.barrier()
			c.emit(r)
			continue
		}
		c.submit(r)
	}
	if c.ctx.Err() == nil {
		c.barrier()
	}
	c.closeStreams()
	go func() {
		for range req {
		}
	}()
}

type target struct {
	src   *Source
	probe bool
}

func (c *lookupCall) targets(id string) []target {
	if id == "" {
		return nil
	}
	owner := c.g.owner(id)
	if owner == nil {
		log.Debugf("no source owns %s", id)
		return nil
	}
	out := []target{{owner, false}}
	if c.spec.fanout {
		for _, s := range c.g.inbound(owner.Prefix, c.spec.labels) {
			out = append(out, target{s, true})
		}
	}
	return out
}

func (c *lookupCall) newID() uint64 {
	c.nextID++
	return c.nextID
}

func (c *lookupCall) submit(r gdbi.ElementLookup) {
	targets := c.targets(r.ID)
	if len(targets) == 0 {
		if c.spec.emitNull {
			c.emitNull(r)
		}
		return
	}
	c.mu.Lock()
	rid := c.newID()
	c.pending[rid] = &pendingLookup{lk: r, remaining: len(targets)}
	c.mu.Unlock()
	for _, t := range targets {
		ls, err := c.mainStream(t.src)
		if err != nil {
			c.fail(err)
			return
		}
		in := &TravelerIn{RequestId: rid, Cursor: 1, Current: &Element{Id: r.ID}, ForeignProbe: t.probe, NeedsResolve: c.spec.resolve}
		c.add(ls, in)
	}
}

func (c *lookupCall) emitNull(r gdbi.ElementLookup) {
	r.Vertex = nil
	r.Edge = nil
	c.emit(r)
}

func (c *lookupCall) mainStream(src *Source) (*lookupStream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ls, ok := c.streams[src.Name]; ok {
		return ls, nil
	}
	ls, err := c.openLocked(src, c.spec.steps, false)
	if err == nil {
		c.streams[src.Name] = ls
	}
	return ls, err
}

func (c *lookupCall) resolverStream(src *Source) (*lookupStream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ls, ok := c.resolvers[src.Name]; ok {
		return ls, nil
	}
	steps := []*gripql.GraphStatement{vStatement()}
	ls, err := c.openLocked(src, steps, true)
	if err == nil {
		c.resolvers[src.Name] = ls
	}
	return ls, err
}

func (c *lookupCall) openLocked(src *Source, steps []*gripql.GraphStatement, resolve bool) (*lookupStream, error) {
	s, err := OpenStream(c.ctx, src, &Start{Steps: steps, EntryCursors: []uint32{1}, LoadFinal: true})
	if err != nil {
		return nil, err
	}
	ls := &lookupStream{BatchStream: NewBatchStream(s, lookupBatchSize), resolve: resolve}
	c.consumers.Add(1)
	go c.consume(ls)
	return ls, nil
}

func (c *lookupCall) add(ls *lookupStream, in *TravelerIn) {
	if ls.Add(in) {
		c.sendBatch(ls)
	}
}

func (c *lookupCall) sendBatch(ls *lookupStream) {
	err := ls.Flush(func(int) {
		c.mu.Lock()
		ls.outstanding++
		c.mu.Unlock()
	})
	if err != nil {
		c.fail(err)
	}
}

func (c *lookupCall) allStreams() []*lookupStream {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*lookupStream, 0, len(c.streams)+len(c.resolvers))
	for _, s := range c.streams {
		out = append(out, s)
	}
	for _, s := range c.resolvers {
		out = append(out, s)
	}
	return out
}

func (c *lookupCall) flushAll() {
	for _, ls := range c.allStreams() {
		c.sendBatch(ls)
	}
}

// waitIdle blocks until the given set of streams has no unacknowledged flushes.
func (c *lookupCall) waitIdle(resolvers bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		busy := false
		set := c.streams
		if resolvers {
			set = c.resolvers
		}
		for _, ls := range set {
			if ls.outstanding > 0 {
				busy = true
			}
		}
		if !busy || c.err != nil || c.ctx.Err() != nil {
			return
		}
		c.cond.Wait()
	}
}

// barrier waits until every lookup submitted so far has been fully answered. Main
// streams go first because their results feed the resolver streams.
func (c *lookupCall) barrier() {
	for _, ls := range c.snapshot(false) {
		c.sendBatch(ls)
	}
	c.waitIdle(false)
	for _, ls := range c.snapshot(true) {
		c.sendBatch(ls)
	}
	c.waitIdle(true)
}

func (c *lookupCall) snapshot(resolvers bool) []*lookupStream {
	c.mu.Lock()
	defer c.mu.Unlock()
	set := c.streams
	if resolvers {
		set = c.resolvers
	}
	out := make([]*lookupStream, 0, len(set))
	for _, s := range set {
		out = append(out, s)
	}
	return out
}

func (c *lookupCall) closeStreams() {
	failed := c.failure() != nil || c.ctx.Err() != nil
	for _, ls := range c.allStreams() {
		if failed {
			ls.S.Cancel()
		} else {
			ls.S.CloseSend()
		}
	}
	c.consumers.Wait()
}

func (c *lookupCall) failure() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *lookupCall) consume(ls *lookupStream) {
	defer c.consumers.Done()
	for {
		e, ok := ls.S.Next()
		if !ok {
			return
		}
		switch {
		case e.Err != nil:
			if c.ctx.Err() == nil {
				c.fail(e.Err)
			}
			return
		case e.Closed:
			return
		case e.Result != nil:
			if ls.resolve {
				c.onResolved(e.Result)
			} else {
				c.onResult(e.Result)
			}
		default:
			if id, ok := e.IsAck(); ok {
				c.onAck(ls, id)
			}
		}
	}
}

func (c *lookupCall) onResult(out *TravelerOut) {
	c.mu.Lock()
	p := c.pending[out.RequestId]
	if p != nil {
		p.found = true
	}
	c.mu.Unlock()
	if p == nil || out.Current == nil {
		return
	}
	switch {
	case c.spec.mode == lookupEdges:
		c.emit(gdbi.ElementLookup{Ref: p.lk.Ref, Edge: ProtoToElement(out.Current)})
	case out.Handoff:
		c.resolveForeign(p.lk, out.Current.Id)
	default:
		e := ProtoToElement(out.Current)
		c.emit(gdbi.ElementLookup{ID: e.ID, Ref: p.lk.Ref, Vertex: e})
	}
}

func (c *lookupCall) resolveForeign(lk gdbi.ElementLookup, id string) {
	owner := c.g.owner(id)
	if owner == nil {
		log.Debugf("dropping edge to %s: no source owns it", id)
		return
	}
	rs, err := c.resolverStream(owner)
	if err != nil {
		c.fail(err)
		return
	}
	c.mu.Lock()
	rid := c.newID()
	c.resolving[rid] = lk
	c.mu.Unlock()
	c.add(rs, &TravelerIn{RequestId: rid, Cursor: 1, Current: &Element{Id: id}, NeedsResolve: true})
}

func (c *lookupCall) onResolved(out *TravelerOut) {
	c.mu.Lock()
	lk, ok := c.resolving[out.RequestId]
	c.mu.Unlock()
	if !ok || out.Current == nil {
		return
	}
	e := ProtoToElement(out.Current)
	c.emit(gdbi.ElementLookup{ID: e.ID, Ref: lk.Ref, Vertex: e})
}

func (c *lookupCall) onAck(ls *lookupStream, id uint64) {
	ins := ls.Acked(id)

	var nulls []gdbi.ElementLookup
	c.mu.Lock()
	for _, in := range ins {
		rid := in.RequestId
		if ls.resolve {
			delete(c.resolving, rid)
			continue
		}
		p := c.pending[rid]
		if p == nil {
			continue
		}
		p.remaining--
		if p.remaining == 0 {
			delete(c.pending, rid)
			if !p.found && c.spec.emitNull {
				nulls = append(nulls, p.lk)
			}
		}
	}
	ls.outstanding--
	c.mu.Unlock()
	c.cond.Broadcast()
	for _, lk := range nulls {
		c.emitNull(lk)
	}
}

// scan runs steps on a source from a single seed traveler and sends every
// completed result to the returned channel.
func (g *FederatedGraph) scan(ctx context.Context, src *Source, steps []*gripql.GraphStatement) (<-chan *TravelerOut, error) {
	s, err := OpenStream(ctx, src, &Start{Steps: steps, EntryCursors: []uint32{0}, LoadFinal: true})
	if err != nil {
		return nil, err
	}
	if _, err := s.SendFlushed([]*TravelerIn{{RequestId: 1}}); err != nil {
		s.Cancel()
		return nil, err
	}
	out := make(chan *TravelerOut, 100)
	go func() {
		defer close(out)
		defer s.Cancel()
		for {
			e, ok := s.Next()
			if !ok || e.Closed {
				return
			}
			if e.Err != nil {
				if ctx.Err() == nil {
					log.Errorf("scan of %s failed: %s", src.Name, e.Err)
				}
				return
			}
			if _, isAck := e.IsAck(); isAck {
				return
			}
			if e.Result != nil && !e.Result.Handoff {
				select {
				case out <- e.Result:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func vStatement(ids ...string) *gripql.GraphStatement {
	return gripql.NewQuery().V(ids...).Statements[0]
}

var errReadOnly = fmt.Errorf("federated graph is read only")
