package gripper

import (
  "context"
  "os/exec"
  grpc "google.golang.org/grpc"
  "github.com/hashicorp/go-plugin"
)

var Handshake = plugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "GRIP_PLUGIN_EXTERNAL_RESOURCE",
	MagicCookieValue: "gripper",
}

type GripPlugin struct {
  plugin.Plugin
  Impl GRIPSourceServer
}

var PluginMap = map[string]plugin.Plugin{
	"gripper": &GripPlugin{},
}

func (p *GripPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	RegisterGRIPSourceServer(s, p.Impl)
	return nil
}

func (p *GripPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return NewGRIPSourceClient(c), nil
}


func LaunchPluginClient(name string, params map[string]string) (*plugin.Client, error) {
  client := plugin.NewClient(&plugin.ClientConfig{
    HandshakeConfig: Handshake,
    Plugins:         PluginMap,
    Cmd:             exec.Command("./grip_pfb", "export_2021-06-24T18_56_28.avro"), //FIXME
    AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
  })
  _, err := client.Start()
  return client, err
}

func GetSourceInterface(client *plugin.Client) (GRIPSourceClient, error){
  // Connect via GRPC
  rpcClient, err := client.Client()
  if err != nil {
    return nil, err
  }
  // Request the plugin
  raw, err := rpcClient.Dispense("gripper")
  if err != nil {
    return nil, err
  }
  sourceClient := raw.(GRIPSourceClient)
  return sourceClient, nil
}
