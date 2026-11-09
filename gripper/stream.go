package gripper

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// StreamEvent is something received from a source, in the order the source sent it.
// Exactly one of Result, an ack (see IsAck), Err or Closed is set.
type StreamEvent struct {
	Result *TravelerOut
	ack    bool
	ackID  uint64
	Err    error // terminal error; no events follow
	Closed bool  // the source closed the stream normally; no events follow
}

// IsAck reports whether the event is a flush acknowledgement and returns its id.
func (e StreamEvent) IsAck() (uint64, bool) { return e.ackID, e.ack }

// SourceStream is the client side of one Traverse call. Receiving never blocks on
// the consumer: events are queued without bound so that a consumer which sends
// to other streams while handling an event cannot deadlock the connection.
type SourceStream struct {
	source *Source
	stream GRIPSource_TraverseClient
	cancel context.CancelFunc

	sendMu    sync.Mutex
	nextFlush uint64

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []StreamEvent
	closed bool
}

// OpenStream starts a Traverse call on the source and sends the Start message.
func OpenStream(ctx context.Context, src *Source, start *Start) (*SourceStream, error) {
	ctx, cancel := context.WithCancel(ctx)
	stream, err := src.Client.Traverse(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("source %s: %w", src.Name, err)
	}
	if err := stream.Send(&TraverseRequest{Msg: &TraverseRequest_Start{Start: start}}); err != nil {
		cancel()
		return nil, fmt.Errorf("source %s: %w", src.Name, err)
	}
	s := &SourceStream{source: src, stream: stream, cancel: cancel}
	s.cond = sync.NewCond(&s.mu)
	go s.receive()
	return s, nil
}

func (s *SourceStream) push(e StreamEvent) {
	s.mu.Lock()
	s.queue = append(s.queue, e)
	s.mu.Unlock()
	s.cond.Signal()
}

func (s *SourceStream) receive() {
	for {
		msg, err := s.stream.Recv()
		if err == io.EOF {
			s.push(StreamEvent{Closed: true})
			return
		}
		if err != nil {
			s.push(StreamEvent{Err: fmt.Errorf("source %s: %w", s.source.Name, err)})
			return
		}
		switch m := msg.Msg.(type) {
		case *TraverseResponse_Results:
			for _, r := range m.Results.Results {
				s.push(StreamEvent{Result: r})
			}
		case *TraverseResponse_Ack:
			s.push(StreamEvent{ack: true, ackID: m.Ack.Id})
		case *TraverseResponse_Error:
			s.push(StreamEvent{Err: fmt.Errorf("source %s: %s", s.source.Name, m.Error.Message)})
			return
		}
	}
}

// Next returns the next event, blocking until one is available. It returns false
// once the terminal event (Err or Closed) has been delivered.
func (s *SourceStream) Next() (StreamEvent, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.queue) == 0 {
		if s.closed {
			return StreamEvent{}, false
		}
		s.cond.Wait()
	}
	e := s.queue[0]
	s.queue = s.queue[1:]
	if e.Err != nil || e.Closed {
		s.closed = true
	}
	return e, true
}

// Send submits travelers.
func (s *SourceStream) Send(ins []*TravelerIn) error {
	if len(ins) == 0 {
		return nil
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.stream.Send(&TraverseRequest{Msg: &TraverseRequest_Batch{Batch: &TravelerBatch{Travelers: ins}}})
}

// Flush asks the source to acknowledge once everything sent so far has been
// processed. It returns the id the ack will carry.
func (s *SourceStream) Flush() (uint64, error) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.flushLocked()
}

func (s *SourceStream) flushLocked() (uint64, error) {
	s.nextFlush++
	id := s.nextFlush
	return id, s.stream.Send(&TraverseRequest{Msg: &TraverseRequest_Flush{Flush: &Flush{Id: id}}})
}

// SendFlushed sends travelers followed by a flush and returns the flush id.
func (s *SourceStream) SendFlushed(ins []*TravelerIn) (uint64, error) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if err := s.stream.Send(&TraverseRequest{Msg: &TraverseRequest_Batch{Batch: &TravelerBatch{Travelers: ins}}}); err != nil {
		return 0, err
	}
	return s.flushLocked()
}

// CloseSend tells the source no more travelers follow. Remaining events can still be read.
func (s *SourceStream) CloseSend() error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.stream.CloseSend()
}

// Cancel aborts the stream.
func (s *SourceStream) Cancel() { s.cancel() }

// Source returns the source the stream is connected to.
func (s *SourceStream) Source() *Source { return s.source }

// BatchStream buffers travelers for a SourceStream and sends them as batches, each
// followed by a flush so the ack identifies which travelers have been answered.
type BatchStream struct {
	S   *SourceStream
	Max int

	bmu   sync.Mutex // guards batch
	batch []*TravelerIn

	regMu   sync.Mutex // held while sending so a batch is registered before its ack can be handled
	batches map[uint64][]*TravelerIn
}

func NewBatchStream(s *SourceStream, max int) *BatchStream {
	return &BatchStream{S: s, Max: max, batches: map[uint64][]*TravelerIn{}}
}

// Add buffers a traveler and reports whether the buffer is full and should be flushed.
func (b *BatchStream) Add(in *TravelerIn) bool {
	b.bmu.Lock()
	defer b.bmu.Unlock()
	b.batch = append(b.batch, in)
	return len(b.batch) >= b.Max
}

// Flush sends the buffered travelers. before is called with the batch size just
// before sending, so callers can account for the batch before its ack can arrive.
func (b *BatchStream) Flush(before func(n int)) error {
	b.regMu.Lock()
	defer b.regMu.Unlock()
	b.bmu.Lock()
	batch := b.batch
	b.batch = nil
	b.bmu.Unlock()
	if len(batch) == 0 {
		return nil
	}
	if before != nil {
		before(len(batch))
	}
	id, err := b.S.SendFlushed(batch)
	if err != nil {
		return err
	}
	b.batches[id] = batch
	return nil
}

// Acked returns the travelers of the acknowledged batch.
func (b *BatchStream) Acked(id uint64) []*TravelerIn {
	b.regMu.Lock()
	defer b.regMu.Unlock()
	ins := b.batches[id]
	delete(b.batches, id)
	return ins
}
