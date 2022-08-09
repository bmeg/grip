package accounts

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type StreamOutWrapper[X any] struct {
	Request X
	SS      grpc.ServerStream
}

func NewStreamOutWrapper[X any](inSS grpc.ServerStream) (*StreamOutWrapper[X], error) {
	var req X
	err := inSS.RecvMsg(&req)
	return &StreamOutWrapper[X]{
		req,
		inSS,
	}, err
}

func (bw *StreamOutWrapper[X]) SetHeader(m metadata.MD) error {
	return bw.SS.SendHeader(m)
}

func (bw *StreamOutWrapper[X]) SendHeader(m metadata.MD) error {
	return bw.SS.SendHeader(m)
}

func (bw *StreamOutWrapper[X]) SetTrailer(m metadata.MD) {
	bw.SS.SetTrailer(m)
}

func (bw *StreamOutWrapper[X]) Context() context.Context {
	return bw.SS.Context()
}

func (bw *StreamOutWrapper[X]) SendMsg(m interface{}) error {
	return bw.SS.SendMsg(m)
}

func (bw *StreamOutWrapper[X]) RecvMsg(m interface{}) error {
	mPtr := m.(*X)
	*mPtr = bw.Request
	return nil
}
