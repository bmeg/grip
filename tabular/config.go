package tabular


import (
  "fmt"
	"io/ioutil"
  "path/filepath"
  "github.com/ghodss/yaml"
)

type GraphConfig struct {
  Tables   map[string]TableConfig  `json:"tables"`
  Vertices []VertexConfig          `json:"vertices"`
  Edges    []EdgeConfig            `json:"edges"`
  path    string
}

type TableConfig struct {
  Driver    string       `json:"driver"`
  Path      string       `json:"path"`
}

type VertexConfig struct {
  Table      string      `json:"table"`
  PrimaryKey string      `json:"primaryKey"`
  Label      string      `json:"label"`
  Prefix     string      `json:"prefix"`
}

type EdgeConfig struct {
  ToField   string       `json:"toField"`
  ToTable   string       `json:"toTable"`
  FromField string       `json:"fromField"`
  FromTable string       `json:"fromTable"`
  Label     string       `json:"label"`
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
  conf.path, _ = filepath.Abs(path)
	return conf, nil
}


// Parse parses a YAML doc into the given Config instance.
func ParseConfig(raw []byte, conf *GraphConfig) error {
	return yaml.Unmarshal(raw, conf)
}
