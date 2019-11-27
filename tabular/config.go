package tabular


import (
  "fmt"
	"io/ioutil"
  "path/filepath"
  "github.com/ghodss/yaml"
)

type GraphConfig struct {
  Tables  []TableConfig  `json:"tables"`
  path    string
}

type EdgeConfig struct {
  To        string       `json:"to"`
  ToTable   string       `json:"toTable"`
  From      string       `json:"from"`
  FromTable string       `json:"fromTable"`
  Label     string       `json:"label"`
}

type TableConfig struct {
  Path string            `json:"path"`
  PrimaryKey string      `json:"primaryKey"`
  Label      string      `json:"label"`
  Prefix     string      `json:"prefix"`
  Edges      []EdgeConfig `json:"edges"`
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
