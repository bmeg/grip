package gripper

import (
	"fmt"
	"io/ioutil"

	//"path/filepath"
	"encoding/json"

	"github.com/bmeg/grip/gripql"
	"sigs.k8s.io/yaml"
)

// Config is the component in the global GRIP config file
type Config struct {
	Graph       string
	MappingFile string
	Mapping     *GraphConfig
}

type GraphConfig struct {
	Vertices map[string]VertexConfig `json:"vertices"`
	Edges    map[string]EdgeConfig   `json:"edges"`
}

type ElementConfig struct {
	Source     string `json:"source"`
	Collection string `json:"collection"`
	FromField  string `json:"fromField"`
	ToField    string `json:"toField"`
}

type VertexConfig struct {
	Gid   string        `json:"gid"`
	Label string        `json:"label"`
	Data  ElementConfig `json:"data"`
}

type EdgeConfig struct {
	Gid   string        `json:"gid"`
	To    string        `json:"to"`
	From  string        `json:"from"`
	Label string        `json:"label"`
	Data  ElementConfig `json:"data"`
}

func LoadConfig(path string) (*GraphConfig, error) {
	conf := &GraphConfig{}
	// Read file
	source, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config at path %s: \n%v", path, err)
	}
	// Parse file
	err = ParseConfig(source, conf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config at path %s: \n%v", path, err)
	}
	//conf.path, _ = filepath.Abs(path)
	return conf, nil
}

// Parse parses a YAML doc into the given Config instance.
func ParseConfig(raw []byte, conf *GraphConfig) error {
	return yaml.Unmarshal(raw, conf)
}

func GraphToConfig(graph *gripql.Graph) (*GraphConfig, error) {
	out := GraphConfig{Vertices: map[string]VertexConfig{}, Edges: map[string]EdgeConfig{}}
	for _, vert := range graph.Vertices {
		d := vert.Data.AsMap()
		s, _ := json.Marshal(d)
		vc := VertexConfig{}
		json.Unmarshal(s, &vc)
		vc.Gid = vert.Gid
		vc.Label = vert.Label
		vc.Data = dataToElementConfig(vert.Data.AsMap())
		out.Vertices[vert.Gid] = vc
	}
	for _, edge := range graph.Edges {
		d := edge.Data.AsMap()
		s, _ := json.Marshal(d)
		ec := EdgeConfig{}
		json.Unmarshal(s, &ec)
		ec.Gid = edge.Gid
		ec.Label = edge.Label
		ec.To = edge.To
		ec.From = edge.From
		ec.Data = dataToElementConfig(edge.Data.AsMap())
		out.Edges[edge.Gid] = ec
	}
	return &out, nil
}

func dataToElementConfig(s map[string]interface{}) ElementConfig {
	e := ElementConfig{}
	o, _ := json.Marshal(s)
	json.Unmarshal(o, &e)
	return e
}
