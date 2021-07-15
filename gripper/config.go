package gripper

import (
	"fmt"
	"io/ioutil"

	//"path/filepath"
	"encoding/json"

	"github.com/bmeg/grip/gripql"
	"github.com/ghodss/yaml"
)

// Config is the component in the global GRIP config file
type Config struct {
	Graph      string
	ConfigFile string
}

type GraphConfig struct {
	Vertices map[string]VertexConfig `json:"vertices"`
	Edges    map[string]EdgeConfig   `json:"edges"`
	//path     string
}

type VertexConfig struct {
	Source     string `json:"source"`
	Collection string `json:"collection"`
	Label      string `json:"label"`
}

type FieldToIDConfig struct {
	FromField string `json:"fromField"`
}

type FieldToFieldConfig struct {
	ToField   string `json:"toField"`
	FromField string `json:"fromField"`
}

type EdgeTableConfig struct {
	Source     string `json:"source"`
	Collection string `json:"collection"`
	FromField  string `json:"fromField"`
	ToField    string `json:"toField"`
}

type EdgeConfig struct {
	ToVertex     string              `json:"toVertex"`
	FromVertex   string              `json:"fromVertex"`
	Label        string              `json:"label"`
	FieldToID    *FieldToIDConfig    `json:"fieldToID"`
	FieldToField *FieldToFieldConfig `json:"fieldToField"`
	EdgeTable    *EdgeTableConfig    `json:"edgeTable"`
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
		vc.Label = vert.Label
		out.Vertices[vert.Gid] = vc
	}
	for _, edge := range graph.Edges {
		d := edge.Data.AsMap()
		s, _ := json.Marshal(d)
		ec := EdgeConfig{}
		json.Unmarshal(s, &ec)
		ec.Label = edge.Label
		ec.ToVertex = edge.To
		ec.FromVertex = edge.From
		out.Edges[edge.Gid] = ec
	}
	return &out, nil
}
