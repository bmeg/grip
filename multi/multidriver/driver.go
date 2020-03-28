package multidriver

import (
  "io"
	"context"
	"github.com/bmeg/grip/multi"
  "github.com/bmeg/grip/protoutil"
	"github.com/bmeg/grip/util/rpc"
	"github.com/mitchellh/mapstructure"
	"github.com/bmeg/grip/log"
)

type Config struct {
	Collection string `json:"collection"`
	Host       string `json:"host"`
}

type MultiDriver struct {
	collection string
	man        multi.Cache
	conf       Config
	client     CollectionClient
}

func MultiDriverBuilder(name string, url string, manager multi.Cache, opts multi.Options) (multi.Driver, error) {
	conf := Config{}
	err := mapstructure.Decode(opts.Config, &conf)
	if err != nil {
		return nil, err
	}

	rpcConf := rpc.ConfigWithDefaults(conf.Host)

	log.Info("Connecting to %s", conf.Host)

	conn, err := rpc.Dial(context.Background(), rpcConf)
	if err != nil {
		log.Error("RPC Connection error: %s", err)
		return nil, err
	}

	client := NewCollectionClient(conn)

	o := MultiDriver{man: manager, conf: conf, client: client}
	return &o, nil
}

var loaded = multi.AddDriver("multi", MultiDriverBuilder)

func (m *MultiDriver) GetIDs(ctx context.Context) chan string {
	out := make(chan string, 10)
	go func() {
		defer close(out)
		//out, err := m.client.GetIDs(ctx)
    //return
	}()
	return out
}

func (m *MultiDriver) GetRows(ctx context.Context) chan *multi.TableRow {
	out := make(chan *multi.TableRow, 10)
	go func() {
		defer close(out)
		req := CollectionRequest{Collection: m.conf.Collection}
		cl, err := m.client.GetRows(ctx, &req)
    if err != nil {
      log.WithFields(log.Fields{"error": err}).Error("Receiving traversal result")
      return
    }
		for {
			t, err := cl.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("Receiving traversal result")
				return
			}
      o := multi.TableRow{Key:t.Id, Values:protoutil.AsMap(t.Data)}
			out <- &o
		}
	}()
	return out
}

func (m *MultiDriver) GetRowByID(id string) (*multi.TableRow, error) {
	return nil, nil
}

func (m *MultiDriver) GetRowsByField(ctx context.Context, field string, value string) chan *multi.TableRow {
	out := make(chan *multi.TableRow, 10)
	go func() {
		defer close(out)
	}()
	return out
}
