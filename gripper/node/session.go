package node

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/bmeg/grip/engine"
	"github.com/bmeg/grip/gdbi"
	"github.com/bmeg/grip/gripper"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// outMsg is one item in the ordered output queue of a stream. Results and
// flush acks share the queue so an ack is never sent before earlier results.
type outMsg struct {
	result *gripper.TravelerOut
	ack    *gripper.FlushAck
	err    *gripper.Error
}

type session struct {
	server  *Server
	ctx     context.Context
	cancel  context.CancelFunc
	steps   []*gripql.GraphStatement
	entries map[int]bool
	man     gdbi.Manager

	results chan outMsg
	sem     chan struct{}
	wg      sync.WaitGroup
	pending []chan struct{} // completion signals of started batches, touched by the receive loop only

	mu     sync.Mutex
	closed bool
	err    error
}

// Traverse runs a segment of steps for the travelers streamed by the client.
func (s *Server) Traverse(stream gripper.GRIPSource_TraverseServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "first message must be Start")
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	sess, err := s.newSession(ctx, cancel, start)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	defer sess.man.Cleanup()

	sendDone := make(chan error, 1)
	go func() { sendDone <- sess.sendLoop(stream) }()

	recvDone := make(chan error, 1)
	go func() { recvDone <- sess.recvLoop(stream) }()
	var recvErr error
	select {
	case recvErr = <-recvDone:
	case <-ctx.Done():
		recvErr = ctx.Err()
	}

	sess.mu.Lock()
	sess.closed = true
	sess.mu.Unlock()
	sess.wg.Wait()
	close(sess.results)
	sendErr := <-sendDone

	if err := sess.failure(); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if recvErr != nil {
		return recvErr
	}
	return sendErr
}

func (s *Server) newSession(ctx context.Context, cancel context.CancelFunc, start *gripper.Start) (*session, error) {
	if len(start.Steps) > s.opts.MaxSteps {
		return nil, fmt.Errorf("segment has %d steps, limit is %d", len(start.Steps), s.opts.MaxSteps)
	}
	cursors := make([]int, 0, len(start.EntryCursors))
	for _, c := range start.EntryCursors {
		cursors = append(cursors, int(c))
	}
	if len(cursors) == 0 {
		for i := 0; i <= len(start.Steps); i++ {
			if i == 0 && (len(start.Steps) == 0 || classifyMust(start.Steps[0]) != kindStart) {
				continue
			}
			cursors = append(cursors, i)
		}
	}
	if err := ValidateSteps(start.Steps, cursors); err != nil {
		return nil, err
	}
	inflight := s.opts.MaxInflight
	if m := int(start.MaxInflight); m > 0 && m < inflight {
		inflight = m
	}
	entries := map[int]bool{}
	for _, c := range cursors {
		entries[c] = true
	}
	return &session{
		server:  s,
		ctx:     ctx,
		cancel:  cancel,
		steps:   start.Steps,
		entries: entries,
		man:     engine.NewManager(s.opts.WorkDir),
		results: make(chan outMsg, 1024),
		sem:     make(chan struct{}, inflight),
	}, nil
}

// fail records the first error and aborts the stream. Only goroutines tracked by
// the wait group may ask for the error to be sent to the client.
func (sess *session) fail(err error, notify bool) {
	sess.mu.Lock()
	first := sess.err == nil
	if first {
		sess.err = err
	}
	sess.mu.Unlock()
	if first {
		log.Errorf("Traverse failed: %s", err)
		if notify {
			sess.emit(outMsg{err: &gripper.Error{Message: err.Error()}})
		}
		sess.cancel()
	}
}

func (sess *session) failure() error {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.err
}

// track registers a goroutine with the wait group unless the stream is shutting down.
func (sess *session) track() bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.closed {
		return false
	}
	sess.wg.Add(1)
	return true
}

func (sess *session) recvLoop(stream gripper.GRIPSource_TraverseServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			sess.cancel()
			return err
		}
		switch m := req.Msg.(type) {
		case *gripper.TraverseRequest_Batch:
			if len(m.Batch.Travelers) > maxTravelersPerMsg {
				sess.fail(fmt.Errorf("batch of %d travelers exceeds limit %d", len(m.Batch.Travelers), maxTravelersPerMsg), false)
				return status.Error(codes.InvalidArgument, "batch too large")
			}
			if err := sess.startBatch(m.Batch.Travelers); err != nil {
				return err
			}
		case *gripper.TraverseRequest_Flush:
			sess.startFlush(m.Flush.Id)
		default:
			sess.fail(fmt.Errorf("unexpected message %T", req.Msg), false)
			return status.Error(codes.InvalidArgument, "unexpected message")
		}
	}
}

func (sess *session) startBatch(travelers []*gripper.TravelerIn) error {
	if err := sess.checkBatch(travelers); err != nil {
		sess.fail(err, false)
		return status.Error(codes.InvalidArgument, err.Error())
	}
	select {
	case sess.sem <- struct{}{}:
	case <-sess.ctx.Done():
		return sess.ctx.Err()
	}
	done := make(chan struct{})
	live := sess.pending[:0]
	for _, p := range sess.pending {
		select {
		case <-p:
		default:
			live = append(live, p)
		}
	}
	sess.pending = append(live, done)
	if !sess.track() {
		<-sess.sem
		close(done)
		return nil
	}
	go func() {
		defer sess.wg.Done()
		defer func() { <-sess.sem }()
		defer close(done)
		if err := sess.runBatch(travelers); err != nil && sess.ctx.Err() == nil {
			sess.fail(err, true)
		}
	}()
	return nil
}

func (sess *session) startFlush(id uint64) {
	waitFor := append([]chan struct{}(nil), sess.pending...)
	if !sess.track() {
		return
	}
	go func() {
		defer sess.wg.Done()
		for _, d := range waitFor {
			select {
			case <-d:
			case <-sess.ctx.Done():
				return
			}
		}
		sess.emit(outMsg{ack: &gripper.FlushAck{Id: id}})
	}()
}

func (sess *session) emit(m outMsg) {
	select {
	case sess.results <- m:
	case <-sess.ctx.Done():
	}
}

func (sess *session) checkBatch(travelers []*gripper.TravelerIn) error {
	for _, t := range travelers {
		c := int(t.Cursor)
		if c > len(sess.steps) {
			return fmt.Errorf("traveler %d: cursor %d beyond segment of %d steps", t.RequestId, c, len(sess.steps))
		}
		if c < len(sess.steps) && !sess.entries[c] {
			return fmt.Errorf("traveler %d: cursor %d is not a declared entry cursor", t.RequestId, c)
		}
		if t.Current == nil {
			if c != 0 {
				return fmt.Errorf("traveler %d: only seeds at cursor 0 may omit the current element", t.RequestId)
			}
			continue
		}
		if c == 0 {
			return fmt.Errorf("traveler %d: cursor 0 only accepts seeds", t.RequestId)
		}
		if t.ForeignProbe && c < len(sess.steps) {
			k, fanout, _ := classify(sess.steps[c])
			if !fanout || (k != kindVertexMove && k != kindEdgeMove) {
				return fmt.Errorf("traveler %d: foreign probe requires an in/inE/both/bothE step at cursor %d", t.RequestId, c)
			}
		}
		if t.ForeignProbe && t.NeedsResolve {
			return fmt.Errorf("traveler %d: foreign probe cannot be resolved", t.RequestId)
		}
	}
	return nil
}

// runBatch processes one batch of travelers, grouped by entry cursor.
func (sess *session) runBatch(travelers []*gripper.TravelerIn) error {
	groups := map[int][]*gripper.TravelerIn{}
	for _, t := range travelers {
		groups[int(t.Cursor)] = append(groups[int(t.Cursor)], t)
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(groups))
	for cursor, group := range groups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sess.runGroup(cursor, group); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	return <-errs
}

func (sess *session) runGroup(cursor int, group []*gripper.TravelerIn) error {
	input, err := sess.entryTravelers(group)
	if err != nil {
		return err
	}
	handoff := func(t gdbi.Traveler, c uint32) { sess.emitTraveler(t, c, true) }
	procs := []gdbi.Processor{}
	if cursor < len(sess.steps) {
		procs, err = compileSegment(sess.server.boundary, sess.server.opts.Prefix, sess.steps, cursor, handoff)
		if err != nil {
			return fmt.Errorf("compiling segment at cursor %d: %w", cursor, err)
		}
	}
	for t := range runProcessors(sess.ctx, sess.man, procs, input) {
		if t.IsSignal() {
			continue
		}
		sess.emitTraveler(t, uint32(len(sess.steps)), false)
	}
	return nil
}

func (sess *session) emitTraveler(t gdbi.Traveler, cursor uint32, handoff bool) {
	bt, ok := t.(*gdbi.BaseTraveler)
	if !ok {
		sess.fail(fmt.Errorf("unexpected traveler type %T", t), true)
		return
	}
	out, err := gripper.TravelerToOut(bt.RequestID, cursor, t)
	if err != nil {
		sess.fail(err, true)
		return
	}
	out.Handoff = handoff
	sess.emit(outMsg{result: out})
}

// entryTravelers turns wire travelers into pipeline input, loading the current
// vertex of the ones that ask for it. Travelers whose vertex is missing are dropped.
func (sess *session) entryTravelers(group []*gripper.TravelerIn) (<-chan gdbi.Traveler, error) {
	out := make(chan gdbi.Traveler, len(group))
	var resolve []*gripper.TravelerIn
	for _, in := range group {
		switch {
		case in.Current == nil:
			out <- &gdbi.BaseTraveler{RequestID: in.RequestId}
		case in.NeedsResolve:
			resolve = append(resolve, in)
		default:
			t := gdbi.NewEntryTraveler(gripper.ProtoToElement(in.Current))
			t.RequestID = in.RequestId
			out <- t
		}
	}
	if len(resolve) > 0 {
		req := make(chan gdbi.ElementLookup, len(resolve))
		for _, in := range resolve {
			req <- gdbi.ElementLookup{ID: in.Current.Id, Ref: &gdbi.BaseTraveler{RequestID: in.RequestId}}
		}
		close(req)
		for r := range sess.server.graph.GetVertexChannel(sess.ctx, req, true) {
			if r.Vertex == nil {
				continue
			}
			t := gdbi.NewEntryTraveler(r.Vertex.Get())
			t.RequestID = r.Ref.(*gdbi.BaseTraveler).RequestID
			out <- t
		}
	}
	close(out)
	return out, nil
}

func (sess *session) sendLoop(stream gripper.GRIPSource_TraverseServer) error {
	max := sess.server.opts.MaxBatch
	var sendErr error
	send := func(r *gripper.TraverseResponse) {
		if sendErr != nil {
			return
		}
		if err := stream.Send(r); err != nil {
			sendErr = err
			sess.cancel()
		}
	}
	for m := range sess.results {
		batch := []*gripper.TravelerOut{}
		var tail *outMsg
		cur := m
	collect:
		for {
			if cur.result == nil {
				tail = &cur
				break
			}
			batch = append(batch, cur.result)
			if len(batch) >= max {
				break
			}
			select {
			case n, ok := <-sess.results:
				if !ok {
					break collect
				}
				cur = n
			default:
				break collect
			}
		}
		if len(batch) > 0 {
			send(&gripper.TraverseResponse{Msg: &gripper.TraverseResponse_Results{Results: &gripper.ResultBatch{Results: batch}}})
		}
		if tail != nil {
			switch {
			case tail.ack != nil:
				send(&gripper.TraverseResponse{Msg: &gripper.TraverseResponse_Ack{Ack: tail.ack}})
			case tail.err != nil:
				send(&gripper.TraverseResponse{Msg: &gripper.TraverseResponse_Error{Error: tail.err}})
			}
		}
	}
	return sendErr
}
