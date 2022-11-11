package accounts

import (
	"context"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type BulkWriteFilter struct {
	SS     grpc.ServerStream
	User   string
	Access Access
}

func (bw *BulkWriteFilter) SetHeader(m metadata.MD) error {
	return bw.SS.SendHeader(m)
}

func (bw *BulkWriteFilter) SendHeader(m metadata.MD) error {
	return bw.SS.SendHeader(m)
}

func (bw *BulkWriteFilter) SetTrailer(m metadata.MD) {
	bw.SS.SetTrailer(m)
}

func (bw *BulkWriteFilter) Context() context.Context {
	return bw.SS.Context()
}

func (bw *BulkWriteFilter) SendMsg(m interface{}) error {
	return bw.SS.SendMsg(m)
}

func (bw *BulkWriteFilter) RecvMsg(m interface{}) error {
	for {
		var ge gripql.GraphElement
		err := bw.SS.RecvMsg(&ge)
		if err != nil {
			return err
		}
		err = bw.Access.Enforce(bw.User, ge.Graph, Write)
		if err == nil {
			mPtr := m.(*gripql.GraphElement)
			*mPtr = ge
			return nil
		} else {
			log.Infof("Graph write error: %s", ge.Graph)
		}
	}
}
