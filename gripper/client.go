package gripper

import (
	"context"
	"io"

	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/rpc"
)

/*
type TableConfig struct {
	Collection string `json:"collection"`
	Host       string `json:"host"`
}
*/

// GripperClient manages the multiple connections to named Dig sources
type GripperClient struct {
	confs   map[string]string
	clients map[string]GRIPSourceClient
}

func NewGripperClient(confs map[string]string) *GripperClient {
	o := GripperClient{confs: confs, clients: map[string]GRIPSourceClient{}}
	return &o
}

func (m *GripperClient) startConn(name string) (GRIPSourceClient, error) {
	host := m.confs[name]

	rpcConf := rpc.ConfigWithDefaults(host)
	log.Infof("Connecting to %s", host)
	conn, err := rpc.Dial(context.Background(), rpcConf)
	if err != nil {
		log.Errorf("RPC Connection error: %s", err)
		return nil, err
	}
	client := NewGRIPSourceClient(conn)
	m.clients[name] = client
	return client, nil
}

func (m *GripperClient) getConn(name string) (GRIPSourceClient, error) {
	if c, ok := m.clients[name]; ok {
		return c, nil
	}
	return m.startConn(name)
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
