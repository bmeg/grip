package server

import (
  "strings"
  "context"
  "github.com/bmeg/grip/gripql"
  "github.com/hashicorp/go-plugin"
)


func (server *GripServer) StartPlugin(ctx context.Context, config *gripql.PluginConfig) (*gripql.PluginStatus, error) {
  return &gripql.PluginStatus{}, nil
}

func (server *GripServer) ListDrivers(context.Context, *gripql.Empty) (*gripql.ListDriversResponse, error) {
  list, err := plugin.Discover("gripper-", server.conf.Server.PluginDir)
  out := []string{}
  if err == nil {
      for _, i := range list {
        n := strings.TrimPrefix(i, "gripper-")
        out = append(out, n)
      }
  }
  return &gripql.ListDriversResponse{Drivers:out}, nil
}
