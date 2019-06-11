package gen3

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/ghodss/yaml"
)

type link struct {
	Name         string `json:"name"`
	Backref      string `json:"backref"`
	Label        string `json:"label"`
	TargetType   string `json:"target_type"`
	Multiplicity string `json:"multiplicity"`
	Required     bool   `json:"required"`
}

type value struct {
	StringVal string
	IntVal    int64
	BoolVal   bool
}

func (v *value) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &v.StringVal); err == nil {
		return nil
	} else if err := json.Unmarshal(data, &v.IntVal); err == nil {
		return nil
	} else if err := json.Unmarshal(data, &v.BoolVal); err == nil {
		return nil
	}
	return fmt.Errorf("Unknown type: %s", data)
}

type property struct {
	Type        typeClass `json:"type"`
	Ref         string    `json:"$ref"`
	SystemAlias string    `json:"systemAlias"`
	Description string    `json:"description"`
	Enum        []value   `json:"enum"`
	Default     value     `json:"default"`
	Format      string    `json:"format"`
}

type propertyElement struct {
	Element property
	Value   string
}

func (w *propertyElement) UnmarshalJSON(data []byte) error {
	s := ""
	e := property{}
	if err := json.Unmarshal(data, &e); err == nil {
		w.Element = e
		return nil
	}
	if err := json.Unmarshal(data, &s); err == nil {
		w.Value = s
		return nil
	}
	return fmt.Errorf("Property not element or string: %s", data)
}

type properties map[string]propertyElement

type schema struct {
	ID         string     `json:"id"`
	Title      string     `json:"title"`
	Type       string     `json:"type"`
	Required   []string   `json:"required"`
	UniqueKeys [][]string `json:"uniqueKeys"`
	Links      []link     `json:"links"`
	Props      properties `json:"properties"`
}

type typeClass struct {
	Type  string
	Types []string
}

func (w *typeClass) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &w.Type); err == nil {
		return nil
	} else if err := json.Unmarshal(data, &w.Types); err == nil {
		return nil
	}
	return fmt.Errorf("Found unknown: %s", data)
}

func loadSchema(path string) (*schema, error) {
	raw, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read data at path %s: %v", path, err)
	}
	s := &schema{}
	if err := yaml.Unmarshal(raw, s); err != nil {
		return nil, fmt.Errorf("failed to read data at path %s: %v", path, err)
	}
	return s, nil
}

func loadAllSchemas(path string, exclude []string) (map[string]*schema, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("provided path '%s', is not a directory", path)
	}
	files, err := filepath.Glob(filepath.Join(path, "*.yaml"))
	if err != nil {
		return nil, err
	}
	out := make(map[string]*schema)
loadLoop:
	for _, f := range files {
		for _, pattern := range exclude {
			if strings.Contains(filepath.Base(f), pattern) {
				continue loadLoop
			}
		}
		s, err := loadSchema(f)
		if err != nil {
			return nil, fmt.Errorf("error loading schema: %s", err)
		}
		if s.ID == "" {
			return nil, fmt.Errorf("encountered schema with no ID '%s'", f)
		}
		if _, ok := out[s.ID]; ok {
			return nil, fmt.Errorf("encountered multiple schema with the same ID '%s'", s.ID)
		}
		out[s.ID] = s
	}
	return out, nil
}
