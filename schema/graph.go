package schema

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/bmeg/grip/log"

	"slices"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/jsonschema/v5"
	"github.com/bmeg/jsonschemagraph/compile"
	"github.com/bmeg/jsonschemagraph/graph"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"
)

func ParseSchema(schema *jsonschema.Schema) any {
	/* This function traverses through the compiled json schema constructing a simplified
	schema that consists of only golang primitive types */

	//log.Infof("ENTERING FLATTEN SCHEMA %#v\n", schema)
	result := make(map[string]any)
	if schema.Ref != nil && schema.Ref.Title != "" {
		if slices.Contains([]string{"Reference", "FHIRPrimitiveExtension", "Extension", "Link"}, schema.Ref.Title) {
			return nil
		}
		return ParseSchema(schema.Ref)
	}
	if schema.Items2020 != nil {
		if schema.Items2020.Ref != nil &&
			schema.Items2020.Ref.Title != "" &&
			slices.Contains([]string{"Reference", "FHIRPrimitiveExtension", "Extension", "Link", "Link Description Object"}, schema.Items2020.Ref.Title) {
			return nil
		}
		if schema.Types[0] == "array" {
			return []any{ParseSchema(schema.Items2020)}
		}
		return ParseSchema(schema.Items2020)
	}

	if len(schema.Properties) > 0 {
		for key, property := range schema.Properties {
			if val := ParseSchema(property); val != nil {
				result[key] = val
			}
		}
		return result
	}
	if schema.AnyOf != nil {
		return nil
		/* fhir_comments not implemented
		for _, val := range schema.AnyOf {
		return ParseSchema(val)
		}*/
	}
	if schema.Types != nil {
		return schema.Types[0]
	}
	return nil
}

func ParseJSONSchemaGraphs(relpath string) ([]*gripql.Graph, error) {
	graphs := []*gripql.Graph{}

	// register schema extension and compile schemas
	compiler := jsonschema.NewCompiler()
	compiler.ExtractAnnotations = true
	compiler.RegisterExtension(compile.GraphExtensionTag, compile.GraphExtMeta, compile.GraphExtCompiler{})
	out := graph.GraphSchema{Classes: map[string]*jsonschema.Schema{}, Compiler: compiler}
	if sch, err := compiler.Compile(relpath); err == nil {
		for _, obj := range graph.ObjectScan(sch) {
			if obj.Title != "" {
				out.Classes[obj.Title] = obj
			}
		}
	}

	expanded := make(map[string]any)
	for key, value := range out.GetClass("Observation").Properties {
		// Removing FHIRPrimitiveExtension, but it clutters up the schemas alot.
		if value.Ref != nil && value.Ref.Title != "" && slices.Contains([]string{"Reference", "FHIRPrimitiveExtension", "Extension", "Link"}, value.Ref.Title) {
			continue
		}
		flattened_values := ParseSchema(value)
		//log.Info("FLATTENED VALUES: ", flattened_values)
		switch flattened_values.(type) {
		case string:
			expanded[key] = flattened_values.(string)
		case int:
			expanded[key] = flattened_values.(int)
		case map[string]any:
			expanded[key] = flattened_values.(map[string]any)
		case []any:
			expanded[key] = flattened_values.([]any)
		}
	}

	fmt.Println("EXPANDED: ", expanded)
	expandedJSON, err := json.MarshalIndent(expanded, "", "  ")
	if err != nil {
		log.Errorf("Failed to marshal expanded schema: %v", err)
	}
	log.Info(string(expandedJSON))

	return graphs, nil
}

func ParseYAMLSchemaGraphs(source []byte) ([]*gripql.Graph, error) {
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
func parseGraphFile(relpath string, format string) ([]*gripql.Graph, error) {
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
	case "jsonSchema":
		graphs, err = ParseJSONSchemaGraphs(relpath)
	case "yamlSchema":
		graphs, err = ParseYAMLSchemaGraphs(source)
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
func ParseYAMLGraphsFile(relpath string) ([]*gripql.Graph, error) {
	return parseGraphFile(relpath, "yaml")
}

// ParseJSONGraphFile parses a graph file, which is formatted in JSON,
// and returns a slice of graph objects.
func ParseJSONGraphsFile(relpath string) ([]*gripql.Graph, error) {
	return parseGraphFile(relpath, "json")
}

func ParseJSONSchemaGraphsFile(relpath string) ([]*gripql.Graph, error) {
	return parseGraphFile(relpath, "jsonSchema")
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
