package gripper

import (
	"context"

	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/util/rpc"
)

func StartConnection(host string) (GRIPSourceClient, error) {
	rpcConf := rpc.ConfigWithDefaults(host)
	log.Infof("Connecting to %s", host)
	conn, err := rpc.Dial(context.Background(), rpcConf)
	if err != nil {
		log.Errorf("RPC Connection error: %s", err)
		return nil, err
	}
	client := NewGRIPSourceClient(conn)
	return client, nil
}
