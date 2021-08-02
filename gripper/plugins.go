package gripper

import (
  "os"
  "fmt"
  "path/filepath"
  "context"
  "os/exec"
  "io/ioutil"
  "encoding/json"
  grpc "google.golang.org/grpc"
  "github.com/hashicorp/go-plugin"
  "github.com/kennygrant/sanitize"
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


func LaunchPluginClient(plugindir string, name string, workdir string, params map[string]string) (*plugin.Client, error) {
  name = sanitize.BaseName(name)
  plugPath := filepath.Join(plugindir, "gripper-" + name)

  if _, err := os.Stat(plugPath); err != nil {
    return nil, fmt.Errorf("plugin %s not found", name)
  }

  confPath := filepath.Join(workdir, "conf.json")
  message, err := json.Marshal(params)
  if err != nil {
    return nil, err
  }
  err = ioutil.WriteFile(confPath, message, 0644)
	if err != nil {
    return nil, err
  }
  client := plugin.NewClient(&plugin.ClientConfig{
    HandshakeConfig: Handshake,
    Plugins:         PluginMap,
    Cmd:             exec.Command(plugPath, confPath), //FIXME
    AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
  })
  _, err = client.Start()
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
