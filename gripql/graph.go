package gripql

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"
)

// ParseYAMLGraph parses a YAML doc into the given Graph instance.
func ParseYAMLGraph(raw []byte) (*Graph, error) {
	tmp := map[string]interface{}{}
	err := yaml.Unmarshal(raw, &tmp)
	if err != nil {
		return nil, err
	}
	part, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	g := &Graph{}
	err = protojson.Unmarshal(part, g)
	if err != nil {
		return nil, err
	}
	return g, nil
}

func ParseYAMLGraphPath(relpath string) (*Graph, error) {
	// Try to get absolute path. If it fails, fall back to relative path.
	path, err := filepath.Abs(relpath)
	if err != nil {
		path = relpath
	}
	// Read file
	source, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read graph at path %s: \n%v", path, err)
	}
	return ParseYAMLGraph(source)
}

// ParseYAMLGraph parses a YAML doc into the given Graph instance.
func ParseYAMLGraphs(raw []byte) ([]*Graph, error) {
	graphs := []*Graph{}
	tmp := []interface{}{}
	err := yaml.Unmarshal(raw, &tmp)
	if err != nil {
		var tmp2 interface{}
		err := yaml.Unmarshal(raw, &tmp2)
		if err != nil {
			return nil, err
		}
		tmp = append(tmp, tmp2)
	}
	for _, s := range tmp {
		part, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		g := &Graph{}
		err = protojson.Unmarshal(part, g)
		if err != nil {
			return nil, err
		}
		if g.Graph == "" {
			return nil, fmt.Errorf("missing graph name")
		}
		graphs = append(graphs, g)
	}
	return graphs, nil
}

// ParseJSONGraph parses a JSON doc into the given Graph instance.
func ParseJSONGraphs(raw []byte) ([]*Graph, error) {
	graphs := []*Graph{}
	tmp := []interface{}{}
	err := json.Unmarshal(raw, &tmp)
	if err != nil {
		var tmp2 interface{}
		err := json.Unmarshal(raw, &tmp2)
		if err != nil {
			return nil, err
		}
		tmp = append(tmp, tmp2)
	}
	for _, s := range tmp {
		part, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		g := &Graph{}
		err = protojson.Unmarshal(part, g)
		if err != nil {
			return nil, err
		}
		if g.Graph == "" {
			return nil, fmt.Errorf("missing graph name")
		}
		graphs = append(graphs, g)
	}
	return graphs, nil
}

// ParseGraphYAMLFile parses a graph file, which is formatted in YAML,
// and returns a slice of graph objects.
func parseGraphFile(relpath string, format string) ([]*Graph, error) {
	var graphs []*Graph
	var err error

	if relpath == "" {
		return nil, fmt.Errorf("path is empty")
	}

	// Try to get absolute path. If it fails, fall back to relative path.
	path, err := filepath.Abs(relpath)
	if err != nil {
		path = relpath
	}

	// Read file
	source, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read graph at path %s: \n%v", path, err)
	}

	// Parse file contents
	switch format {
	case "yaml":
		graphs, err = ParseYAMLGraphs(source)
	case "json":
		graphs, err = ParseJSONGraphs(source)
	default:
		err = fmt.Errorf("unknown file format: %s", format)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to parse graph at path %s: \n%v", path, err)
	}
	return graphs, nil
}

// ParseYAMLGraphFile parses a graph file, which is formatted in YAML,
// and returns a slice of graph objects.
func ParseYAMLGraphsFile(relpath string) ([]*Graph, error) {
	return parseGraphFile(relpath, "yaml")
}

// ParseJSONGraphFile parses a graph file, which is formatted in JSON,
// and returns a slice of graph objects.
func ParseJSONGraphsFile(relpath string) ([]*Graph, error) {
	return parseGraphFile(relpath, "json")
}

// GraphToYAMLString returns a graph formatted as a YAML string
func GraphToYAMLString(graph *Graph) (string, error) {
	out, err := protojson.Marshal(graph)
	if err != nil {
		return "", fmt.Errorf("failed to marshal graph: %v", err)
	}
	sb, err := yaml.JSONToYAML(out)
	if err != nil {
		return "", fmt.Errorf("failed to marshal graph: %v", err)
	}
	return string(sb), nil
}

// GraphToJSONString returns a graph formatted as a JSON string
func GraphToJSONString(graph *Graph) (string, error) {
	m := protojson.MarshalOptions{
		UseEnumNumbers:  false,
		EmitUnpopulated: false,
		Indent:          "  ",
		UseProtoNames:   false,
	}
	txt := m.Format(graph)
	return txt, nil
}

func GraphMapToProto(data map[string]interface{}) (*Graph, error) {
	part, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	g := &Graph{}
	err = protojson.Unmarshal(part, g)
	if err != nil {
		return nil, err
	}
	return g, nil
}
