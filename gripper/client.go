package gripper

import (
	"context"
	"fmt"
	"io"

	"github.com/bmeg/grip/log"
)

// GripperClient manages the multiple connections to named Dig sources
type GripperClient struct {
	clients map[string]GRIPSourceClient
}

func NewGripperClient(clients map[string]GRIPSourceClient) *GripperClient {
	o := GripperClient{clients: clients}
	return &o
}

func (m *GripperClient) getConn(name string) (GRIPSourceClient, error) {
	if c, ok := m.clients[name]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("%s not found", name)
}

func (m *GripperClient) GetCollectionInfo(ctx context.Context, source string, collection string) (*CollectionInfo, error) {
	client, err := m.getConn(source)
	if err != nil {
		return nil, err
	}
	req := Collection{Name: collection}
	return client.GetCollectionInfo(ctx, &req)
}

func (m *GripperClient) GetCollections(ctx context.Context, source string) chan string {
	out := make(chan string, 10)
	go func() {
		defer close(out)
		client, err := m.getConn(source)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Errorf("Error Connecting to %s", source)
			return
		}
		cl, err := client.GetCollections(ctx, &Empty{})
		if err != nil {
			if ctx.Err() != context.Canceled {
				log.WithFields(log.Fields{"error": err}).Error("Error Receiving collecion list in GetCollections")
			}
			return
		}
		for {
			t, err := cl.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				if ctx.Err() != context.Canceled {
					log.WithFields(log.Fields{"error": err}).Error("Error with cl.Recv in GetCollections")
				}
				return
			}
			out <- t.Name
		}
	}()
	return out
}

func (m *GripperClient) GetIDs(ctx context.Context, source string, collection string) chan string {
	out := make(chan string, 10)
	go func() {
		defer close(out)
		client, err := m.getConn(source)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Errorf("Error Connecting to %s", source)
			return
		}
		req := Collection{Name: collection}
		cl, err := client.GetIDs(ctx, &req)
		if err != nil {
			if ctx.Err() != context.Canceled {
				log.WithFields(log.Fields{"error": err}).Error("Error calling GetIDs")
			}
			return
		}
		for {
			t, err := cl.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				if ctx.Err() != context.Canceled {
					log.WithFields(log.Fields{"error": err}).Error("Error calling cl.Recv in GetIDs")
				}
				return
			}
			out <- t.Id
		}
	}()
	return out
}

func (m *GripperClient) GetRows(ctx context.Context, source string, collection string) chan *Row {
	out := make(chan *Row, 10)
	go func() {
		defer close(out)
		client, err := m.getConn(source)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Errorf("Error Connecting to %s", source)
			return
		}
		req := Collection{Name: collection}
		cl, err := client.GetRows(ctx, &req)
		if err != nil {
			if ctx.Err() != context.Canceled {
				log.WithFields(log.Fields{"error": err}).Error("Error calling GetRows")
			}
			return
		}
		for {
			t, err := cl.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				if ctx.Err() != context.Canceled {
					log.WithFields(log.Fields{"error": err}).Error("Error calling cl.Recv in GetRows")
				}
				return
			}
			out <- t
		}
	}()
	return out
}

func (m *GripperClient) GetRowsByID(ctx context.Context, source string, collection string, reqChan chan *RowRequest) (chan *Row, error) {
	out := make(chan *Row, 10)
	client, err := m.getConn(source)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Errorf("Error Connecting to %s", source)
		return nil, err
	}
	cl, err := client.GetRowsByID(ctx)
	if err != nil {
		return nil, err
	}
	go func() {
		for i := range reqChan {
			req := RowRequest{Collection: collection, Id: i.Id, RequestID: i.RequestID}
			cl.Send(&req)
		}
		cl.CloseSend()
	}()
	go func() {
		defer close(out)
		for {
			t, err := cl.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				if ctx.Err() != context.Canceled {
					log.WithFields(log.Fields{"error": err}).Error("Error calling cl.Recv in GetRowsByID")
				}
				return
			}
			out <- t
		}
	}()
	return out, nil
}

func (m *GripperClient) GetRowsByField(ctx context.Context, source string, collection string, field string, value string) (chan *Row, error) {
	out := make(chan *Row, 10)
	client, err := m.getConn(source)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Errorf("Error Connecting to %s", source)
		return nil, err
	}
	req := FieldRequest{Collection: collection, Field: field, Value: value}
	cl, err := client.GetRowsByField(ctx, &req)
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(out)
		for {
			t, err := cl.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				if ctx.Err() != context.Canceled {
					log.WithFields(log.Fields{"error": err}).Error("Error calling cl.Recv in GetRowsByField")
				}
				return
			}
			out <- t
		}
	}()
	return out, nil
}
