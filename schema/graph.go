package schema

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/bmeg/grip/log"

	"github.com/bmeg/grip/gripql"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"
)

func ParseYAMLSchemaGraphs(source []byte, graphName string) ([]*gripql.Graph, error) {
	return nil, nil
}

// ParseYAMLGraph parses a YAML doc into the given Graph instance.
func ParseYAMLGraph(raw []byte) (*gripql.Graph, error) {
	tmp := map[string]interface{}{}
	err := yaml.Unmarshal(raw, &tmp)
	if err != nil {
		return nil, err
	}
	part, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	g := &gripql.Graph{}
	err = protojson.Unmarshal(part, g)
	if err != nil {
		return nil, err
	}
	return g, nil
}

func ParseYAMLGraphPath(relpath string) (*gripql.Graph, error) {
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
func ParseYAMLGraphs(raw []byte) ([]*gripql.Graph, error) {
	graphs := []*gripql.Graph{}
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
		g := &gripql.Graph{}
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
func ParseJSONGraphs(raw []byte) ([]*gripql.Graph, error) {
	graphs := []*gripql.Graph{}
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
		g := &gripql.Graph{}
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
func parseGraphFile(relpath string, format string, graphName string) ([]*gripql.Graph, error) {
	var graphs []*gripql.Graph
	var err error

	if relpath == "" {
		return nil, fmt.Errorf("path is empty")
	}

	// Try to get absolute path. If it fails, fall back to relative path.
	path, err := filepath.Abs(relpath)
	if err != nil {
		path = relpath
	}

	var source []byte
	if format == "yaml" || format == "json" {
		source, err = ioutil.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read graph at path %s: \n%v", path, err)
		}
	}

	// Parse file contents
	switch format {
	case "yaml":
		graphs, err = ParseYAMLGraphs(source)
	case "json":
		graphs, err = ParseJSONGraphs(source)
	case "jSchema":
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open file: %v", err)
		}
		defer file.Close()

		// Read the entire file content
		bytes, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, fmt.Errorf("failed to read file: %v", err)
		}
		graphs, err = ParseJSchema(bytes, graphName)
	default:
		err = fmt.Errorf("unknown file format: %s", format)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to parse graph at path %s: \n%v", path, err)
	}
	return graphs, nil
}

func ParseJSchema(bytes []byte, graphName string) ([]*gripql.Graph, error) {
	graphSchema := map[string]any{
		"vertices": []map[string]any{},
		"edges":    []map[string]any{},
		"graph":    graphName,
	}

	// Parse JSON into a map
	var data map[string]any
	if err := json.Unmarshal(bytes, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %v", err)
	}
	for key, values := range data["$defs"].(map[string]any) {
		vals := values.(map[string]any)
		if idVal, exists := vals["$id"]; exists {
			delete(vals, "$id")
			vals["id"] = idVal
		}

		vertex := map[string]any{"data": values, "label": key, "gid": data["$id"].(string) + "/" + key}
		graphSchema["vertices"] = append(graphSchema["vertices"].([]map[string]any), vertex)
	}

	expandedJSON, err := json.Marshal(graphSchema)
	if err != nil {
		log.Errorf("Failed to marshal expanded schema: %v", err)
	}
	graphs := gripql.Graph{}
	json.Unmarshal(expandedJSON, &graphs)
	return []*gripql.Graph{&graphs}, nil
}

// ParseYAMLGraphFile parses a graph file, which is formatted in YAML,
// and returns a slice of graph objects.
func ParseYAMLGraphsFile(relpath string) ([]*gripql.Graph, error) {
	return parseGraphFile(relpath, "yaml", "")
}

// ParseJSONGraphFile parses a graph file, which is formatted in JSON,
// and returns a slice of graph objects.
func ParseJSONGraphsFile(relpath string) ([]*gripql.Graph, error) {
	return parseGraphFile(relpath, "json", "")
}

func ParseJsonSchema(relpath string, graphName string) ([]*gripql.Graph, error) {
	return parseGraphFile(relpath, "jSchema", graphName)
}

func ParseYamlJsonSchema(relpath string, graphName string) ([]*gripql.Graph, error) {
	return parseGraphFile(relpath, "yjSchema", graphName)
}

// GraphToYAMLString returns a graph formatted as a YAML string
func GraphToYAMLString(graph *gripql.Graph) (string, error) {
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
func GraphToJSONString(graph *gripql.Graph) (string, error) {
	m := protojson.MarshalOptions{
		UseEnumNumbers:  false,
		EmitUnpopulated: false,
		Indent:          "  ",
		UseProtoNames:   false,
	}
	txt := m.Format(graph)
	return txt, nil
}

func GraphMapToProto(data map[string]interface{}) (*gripql.Graph, error) {
	part, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	g := &gripql.Graph{}
	err = protojson.Unmarshal(part, g)
	if err != nil {
		return nil, err
	}
	return g, nil
}
