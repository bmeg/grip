package schema

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/bmeg/grip/log"

	"slices"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/jsonschema/v5"
	"github.com/bmeg/jsonschemagraph/compile"
	"github.com/bmeg/jsonschemagraph/graph"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"
)

func ConvertToGripqlType(field string) string {
	switch field {
	case "string":
		return gripql.FieldType_STRING.String()
	case "integer":
		return gripql.FieldType_NUMERIC.String()
	case "number":
		return gripql.FieldType_NUMERIC.String()
	case "boolean":
		return gripql.FieldType_BOOL.String()
	default:
		return gripql.FieldType_UNKNOWN.String()
	}
}

func ParseSchema(schema *jsonschema.Schema) any {
	/* This function traverses through the compiled json schema constructing a simplified
	schema that consists of only golang primitive types */

	//log.Infof("ENTERING FLATTEN SCHEMA %#v\n", schema)
	vertData := make(map[string]any)
	if schema.Ref != nil && schema.Ref.Title != "" {
		// Primitive extensions are currently not supported.
		if slices.Contains([]string{"Reference", "Link", "FHIRPrimitiveExtension"}, schema.Ref.Title) {
			return nil
		}
		return ParseSchema(schema.Ref)
	}
	if schema.Items2020 != nil {
		if schema.Items2020.Ref != nil &&
			schema.Items2020.Ref.Title != "" &&
			slices.Contains([]string{"Reference", "Link", "Link Description Object", "FHIRPrimitiveExtension"}, schema.Items2020.Ref.Title) {
			return nil
		}
		if schema.Types[0] == "array" {
			return []any{ParseSchema(schema.Items2020)}
		}
		return ParseSchema(schema.Items2020)
	}

	if len(schema.Properties) > 0 {
		for key, property := range schema.Properties {
			// Not going to support inifinite nested extensions even though FHIR does.
			if key == "extension" || key == "modifierExtension" {
				continue
			}
			if val := ParseSchema(property); val != nil {
				vertData[key] = val
			}
		}
		return vertData
	}
	if schema.AnyOf != nil {
		return nil
		/* fhir_comments not implemented
		for _, val := range schema.AnyOf {
		return ParseSchema(val)
		}*/
	}
	if schema.Types != nil {
		return ConvertToGripqlType(schema.Types[0])
	}
	return nil
}

func ParseSchemaGraphs(relpath string, graphName string) ([]*gripql.Graph, error) {
	out, err := graph.Load(relpath)
	if err != nil {
		log.Info("AN ERROR HAS OCCURED: ", err)
		return nil, err
	}
	graphSchema := map[string]any{
		"vertices": []map[string]any{},
		"edges":    []map[string]any{},
		"graph":    graphName,
	}
	edgeList := []map[string]any{}
	for _, class := range out.Classes {
		// Since reading from schema there should be no duplicate edges
		if ext, ok := class.Extensions[compile.GraphExtensionTag]; ok {
			for _, target := range ext.(compile.GraphExtension).Targets {
				ToVertex := strings.Split(target.Rel, "_")
				edgeList = append(edgeList, map[string]any{
					"gid":   fmt.Sprintf("(%s)-%s->(%s)", class.Title, target.Rel, ToVertex[len(ToVertex)-1]),
					"label": target.Rel,
					"from":  class.Title,
					"to":    ToVertex[len(ToVertex)-1],
					// TODO: No data field supported
				})
			}
		}
		vertexData := make(map[string]any)
		for key, sch := range class.Properties {
			if sch.Ref != nil && sch.Ref.Title != "" && slices.Contains([]string{"Reference", "Link", "FHIRPrimitiveExtension"}, sch.Ref.Title) {
				continue
			}
			vertVal := ParseSchema(sch)
			//log.Info("FLATTENED VALUES: ", flattened_values)
			switch vertVal.(type) {
			case string:
				vertexData[key] = vertVal.(string)
			case int:
				vertexData[key] = vertVal.(int)
			case map[string]any:
				vertexData[key] = vertVal.(map[string]any)
			case []any:
				vertexData[key] = vertVal.([]any)
			}
		}
		vertex := map[string]any{"data": vertexData, "label": "Vertex", "gid": class.Title}
		graphSchema["vertices"] = append(graphSchema["vertices"].([]map[string]any), vertex)
		graphSchema["edges"] = edgeList

	}

	expandedJSON, err := json.Marshal(graphSchema)
	if err != nil {
		log.Errorf("Failed to marshal expanded schema: %v", err)
	}
	/*
		For Testing purposes
		err = os.WriteFile("new_dicts.json", expandedJSON, 0644)
		if err != nil {
			log.Errorf("Failed to write to file: %v", err)
			}
	*/

	graphs := gripql.Graph{}
	json.Unmarshal(expandedJSON, &graphs)
	return []*gripql.Graph{&graphs}, nil
}

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
	case "jsonSchema":
		graphs, err = ParseSchemaGraphs(path, graphName)
	case "yamlSchema":
		graphs, err = ParseSchemaGraphs(relpath, graphName)
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
	return parseGraphFile(relpath, "yaml", "")
}

// ParseJSONGraphFile parses a graph file, which is formatted in JSON,
// and returns a slice of graph objects.
func ParseJSONGraphsFile(relpath string) ([]*gripql.Graph, error) {
	return parseGraphFile(relpath, "json", "")
}

func ParseJSONSchemaGraphsFile(relpath string, graphName string) ([]*gripql.Graph, error) {
	return parseGraphFile(relpath, "jsonSchema", graphName)
}

func ParseYAMLSchemaGraphsFiles(relpath string, graphName string) ([]*gripql.Graph, error) {
	return parseGraphFile(relpath, "jsonSchema", graphName)
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
