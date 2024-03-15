package gripqljs

import (
	"encoding/json"
	"fmt"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/jsengine/underscore"
	"github.com/dop251/goja"
	"google.golang.org/protobuf/encoding/protojson"
)

func ParseQuery(queryString string) (*gripql.GraphQuery, error) {
	vm := goja.New()
	us, err := underscore.Asset("underscore.js")
	if err != nil {
		return nil, fmt.Errorf("failed to load underscore.js")
	}
	if _, err := vm.RunString(string(us)); err != nil {
		return nil, err
	}
	gripqlString, err := Asset("gripql.js")
	if err != nil {
		return nil, fmt.Errorf("failed to load gripql.js")
	}
	if _, err := vm.RunString(string(gripqlString)); err != nil {
		return nil, err
	}

	val, err := vm.RunString(queryString)
	if err != nil {
		return nil, err
	}
	obj := val.ToObject(vm)
	obj.Delete("client")
	queryJSON, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	fmt.Printf("%s\n", queryJSON)
	query := gripql.GraphQuery{}
	err = protojson.Unmarshal(queryJSON, &query)
	if err != nil {
		return nil, err
	}

	return &query, nil
}
