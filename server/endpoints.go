package server

import (
	"fmt"
	"net/http"
	"plugin"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
)

type EndpointSetupFunc func(client gripql.Client) (http.Handler, error)

var endpointMap = map[string]EndpointSetupFunc{}

func (server *GripServer) AddEndpoint(name string, path string) error {

	plg, err := plugin.Open(path)
	if err != nil {
		return err
	}

	gen, err := plg.Lookup("NewHTTPHandler")
	if err != nil {
		return err
	}
	fmt.Printf("Method: %#v\n", gen)
	if x, ok := (gen).(func(client gripql.Client) (http.Handler, error)); ok {
		log.Infof("Plugin %s loaded", path)
		endpointMap[name] = x
		return nil
	}
	return fmt.Errorf("unable to call NewHTTPHandler method")
}
