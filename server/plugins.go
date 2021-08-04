package server

import (
  "fmt"
  "strings"
  "context"
  "io/ioutil"
  "path/filepath"
  "github.com/bmeg/grip/log"
  "github.com/bmeg/grip/gripql"
  "github.com/bmeg/grip/gripper"
  "github.com/hashicorp/go-plugin"
)

type Plugin struct {
  name   string
  plugin *plugin.Client
  client gripper.GRIPSourceClient
}

func (server *GripServer) ClosePlugins() {
  plugin.CleanupClients()
}

func (server *GripServer) StartPlugin(ctx context.Context, config *gripql.PluginConfig) (*gripql.PluginStatus, error) {
  if _, ok := server.plugins[config.Name]; ok {
    return nil, fmt.Errorf("Plugin named %s already running", config.Name)
  }
  workdir, err := ioutil.TempDir(server.conf.Server.WorkDir, "gripper-")
  if err != nil {
    return nil, err
  }
  log.Infof("Starting plugin %s with driver %s", config.Name, config.Driver)
  plg, err := gripper.LaunchPluginClient(server.conf.Server.PluginDir, config.Driver, workdir, config.Config)
  if err != nil {
    return nil, err
  }
  cli, err := gripper.GetSourceInterface(plg)
  if err != nil {
    return nil, err
  }
  server.plugins[config.Name] = &Plugin{name:config.Name, plugin:plg, client:cli}
  server.sources[config.Name] = cli
  return &gripql.PluginStatus{Name: config.Name}, nil
}

func (server *GripServer) ListPlugins(context.Context, *gripql.Empty) (*gripql.ListPluginsResponse, error) {
  out := []string{}
  for k := range server.plugins {
    out = append(out, k)
  }
  return &gripql.ListPluginsResponse{Plugins:out}, nil
}

func (server *GripServer) ListDrivers(context.Context, *gripql.Empty) (*gripql.ListDriversResponse, error) {
  list, err := plugin.Discover("gripper-*", server.conf.Server.PluginDir)
  out := []string{}
  if err == nil {
      for _, i := range list {
        n := strings.TrimPrefix(filepath.Base(i), "gripper-")
        out = append(out, n)
      }
  } else {
    log.Errorf("Plugin list error: %s", err)
  }
  return &gripql.ListDriversResponse{Drivers:out}, nil
}
