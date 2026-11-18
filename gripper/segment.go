package gripper

import (
	"context"
	"sync"
	"time"

	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

const (
	segmentBatchSize  = 512
	segmentFlushEvery = 5 * time.Millisecond
)

// segmentProc is a gdbi.Processor that runs a fixed run of pushable steps on the
// resource nodes. It keeps the full traveler state; nodes only see the current
// element and send back deltas (see DESIGN.md §5).
type segmentProc struct {
	g *FederatedGraph
	// steps as sent to the nodes: the statements of the segment, preceded by placeholder
	// steps (never executed) when the segment does not begin with V.
	steps []*gripql.GraphStatement
	// first is the node cursor of the segment's first real step.
	first uint32
	// seed is true if the segment begins with V.
	seed       bool
	seedLabels []string
	seedIDs    []string
	// fanout marks a first step that must also ask sources holding edges pointing at the vertex.
	fanout       bool
	fanoutLabels []string
	entries      []uint32
}

type segState struct {
	t    *gdbi.BaseTraveler
	refs int // streams still expected to answer for this request
}

type segStream struct {
	*BatchStream
	outstanding int // guarded by segRun.mu
}

type segRun struct {
	p      *segmentProc
	ctx    context.Context
	cancel context.CancelFunc
	out    gdbi.OutPipe

	mu        sync.Mutex
	cond      *sync.Cond
	nextID    uint64
	state     map[uint64]*segState
	streams   map[string]*segStream
	work      int // travelers buffered or sent but not yet acknowledged
	err       error
	consumers sync.WaitGroup
}

func (p *segmentProc) Process(ctx context.Context, man gdbi.Manager, in gdbi.InPipe, out gdbi.OutPipe) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	r := &segRun{
		p: p, ctx: ctx, cancel: cancel, out: out,
		state:   map[uint64]*segState{},
		streams: map[string]*segStream{},
	}
	r.cond = sync.NewCond(&r.mu)
	go func() {
		// wake anything waiting in quiesce when the step is cancelled from outside
		<-ctx.Done()
		r.mu.Lock()
		r.cond.Broadcast()
		r.mu.Unlock()
	}()
	go r.run(in)
	return ctx
}

func (r *segRun) fail(err error) {
	r.mu.Lock()
	if r.err == nil && r.ctx.Err() == nil {
		r.err = err
		log.Errorf("federated segment failed: %s", err)
	}
	r.mu.Unlock()
	r.cancel()
	r.cond.Broadcast()
}

func (r *segRun) emit(t gdbi.Traveler) {
	select {
	case r.out <- t:
	case <-r.ctx.Done():
	}
}

func (r *segRun) run(in gdbi.InPipe) {
	defer r.cancel()

	tickDone := make(chan struct{})
	defer close(tickDone)
	go func() {
		ticker := time.NewTicker(segmentFlushEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.flushAll()
			case <-tickDone:
				return
			case <-r.ctx.Done():
				return
			}
		}
	}()

input:
	for {
		select {
		case t, ok := <-in:
			if !ok {
				break input
			}
			if t.IsSignal() {
				r.quiesce()
				r.emit(t)
				continue
			}
			bt, ok := t.(*gdbi.BaseTraveler)
			if !ok {
				r.fail(errUnsupportedTraveler(t))
				continue
			}
			r.submit(bt)
		case <-r.ctx.Done():
			break input
		}
	}
	if r.ctx.Err() == nil {
		r.quiesce()
	}
	failed := r.failure() != nil || r.ctx.Err() != nil
	for _, s := range r.allStreams() {
		if failed {
			s.S.Cancel()
		} else {
			s.S.CloseSend()
		}
	}
	r.consumers.Wait()
	// Close the output as soon as this step is cancelled (for example by a downstream
	// limit) even if upstream is still sending: a jump loop upstream only ends once
	// its jump traveler channel, fed by our output, closes.
	close(r.out)
	for range in {
	}
}

func (r *segRun) failure() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *segRun) newState(t *gdbi.BaseTraveler, refs int) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	r.state[r.nextID] = &segState{t: t, refs: refs}
	return r.nextID
}

func (r *segRun) submit(t *gdbi.BaseTraveler) {
	if r.p.seed {
		srcs := r.p.g.sourcesFor(r.p.seedLabels, r.p.seedIDs)
		if len(srcs) == 0 {
			return
		}
		rid := r.newState(t, len(srcs))
		for _, s := range srcs {
			r.send(s, &TravelerIn{RequestId: rid, Cursor: 0})
		}
		return
	}
	if t.IsNull() {
		return
	}
	cur := t.GetCurrent().Get()
	owner := r.p.g.owner(cur.ID)
	if owner == nil {
		log.Debugf("dropping traveler at %s: no source owns it", cur.ID)
		return
	}
	el, err := ElementToProto(cur)
	if err != nil {
		r.fail(err)
		return
	}
	type tgt struct {
		src   *Source
		probe bool
	}
	targets := []tgt{{owner, false}}
	if r.p.fanout {
		for _, s := range r.p.g.inbound(owner.Prefix, r.p.fanoutLabels) {
			targets = append(targets, tgt{s, true})
		}
	}
	rid := r.newState(t, len(targets))
	for _, x := range targets {
		r.send(x.src, &TravelerIn{RequestId: rid, Cursor: r.p.first, Current: el, ForeignProbe: x.probe})
	}
}

func (r *segRun) stream(src *Source) (*segStream, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.streams[src.Name]; ok {
		return s, nil
	}
	ss, err := OpenStream(r.ctx, src, &Start{Steps: r.p.steps, EntryCursors: r.p.entries, LoadFinal: true})
	if err != nil {
		return nil, err
	}
	s := &segStream{BatchStream: NewBatchStream(ss, segmentBatchSize)}
	r.streams[src.Name] = s
	r.consumers.Add(1)
	go r.consume(s)
	return s, nil
}

func (r *segRun) send(src *Source, in *TravelerIn) {
	s, err := r.stream(src)
	if err != nil {
		r.fail(err)
		return
	}
	r.mu.Lock()
	r.work++
	r.mu.Unlock()
	if s.Add(in) {
		r.flush(s)
	}
}

func (r *segRun) flush(s *segStream) {
	err := s.Flush(func(int) {
		r.mu.Lock()
		s.outstanding++
		r.mu.Unlock()
	})
	if err != nil {
		r.fail(err)
	}
}

func (r *segRun) allStreams() []*segStream {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*segStream, 0, len(r.streams))
	for _, s := range r.streams {
		out = append(out, s)
	}
	return out
}

func (r *segRun) flushAll() {
	for _, s := range r.allStreams() {
		r.flush(s)
	}
}

// quiesce returns once every traveler submitted so far, and every traveler spawned
// from handoffs, has been fully processed and its results emitted.
func (r *segRun) quiesce() {
	for {
		r.flushAll()
		r.mu.Lock()
		if r.work == 0 || r.err != nil || r.ctx.Err() != nil {
			r.mu.Unlock()
			return
		}
		r.cond.Wait()
		r.mu.Unlock()
	}
}

func (r *segRun) consume(s *segStream) {
	defer r.consumers.Done()
	for {
		e, ok := s.S.Next()
		if !ok {
			return
		}
		switch {
		case e.Err != nil:
			if r.ctx.Err() == nil {
				r.fail(e.Err)
			}
			return
		case e.Closed:
			return
		case e.Result != nil:
			r.onResult(e.Result)
		default:
			if id, ok := e.IsAck(); ok {
				r.onAck(s, id)
			}
		}
	}
}

func (r *segRun) onResult(out *TravelerOut) {
	r.mu.Lock()
	st := r.state[out.RequestId]
	r.mu.Unlock()
	if st == nil {
		log.Debugf("result for unknown request %d", out.RequestId)
		return
	}
	nt := ApplyTravelerOut(st.t, out)
	if !out.Handoff {
		r.emit(nt)
		return
	}
	owner := r.p.g.owner(out.Current.Id)
	if owner == nil {
		log.Debugf("dropping traveler at %s: no source owns it", out.Current.Id)
		return
	}
	rid := r.newState(nt, 1)
	r.send(owner, &TravelerIn{RequestId: rid, Cursor: out.Cursor, Current: out.Current, NeedsResolve: true})
}

func (r *segRun) onAck(s *segStream, id uint64) {
	ins := s.Acked(id)
	r.mu.Lock()
	for _, in := range ins {
		if st := r.state[in.RequestId]; st != nil {
			st.refs--
			if st.refs <= 0 {
				delete(r.state, in.RequestId)
			}
		}
	}
	r.work -= len(ins)
	s.outstanding--
	r.mu.Unlock()
	r.cond.Broadcast()
}
