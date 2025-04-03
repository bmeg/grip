package schema

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/jsonschema/v5"
	"github.com/bmeg/jsonschemagraph/compile"
	"github.com/bmeg/jsonschemagraph/graph"
)

/* Need some way to denote in the schema Main Nodes, and some way in the schema to denote if the field should be indexed or not */
var NodesMap = map[string]struct{}{
	"Organization":             {},
	"Group":                    {},
	"Practitioner":             {},
	"PractitionerRole":         {},
	"ResearchStudy":            {},
	"Patient":                  {},
	"ResearchSubject":          {},
	"Substance":                {},
	"SubstanceDefinition":      {},
	"Specimen":                 {},
	"Observation":              {},
	"DiagnosticReport":         {},
	"Condition":                {},
	"Medication":               {},
	"MedicationAdministration": {},
	"MedicationStatement":      {},
	"MedicationRequest":        {},
	"Procedure":                {},
	"DocumentReference":        {},
	"Task":                     {},
	"ImagingStudy":             {},
	"FamilyMemberHistory":      {},
	"BodyStructure":            {},
}

func LoadSchemas(sch *gripql.Graph, out *graph.GraphSchema) (*graph.GraphSchema, error) {
	schcompiler := jsonschema.NewCompiler()
	schcompiler.ExtractAnnotations = true
	schcompiler.RegisterExtension(compile.GraphExtensionTag, compile.GraphExtMeta, compile.GraphExtCompiler{})

	for _, v := range sch.Vertices {
		jsonData, err := json.Marshal(v.Data)
		if err != nil {
			return nil, err
		}
		err = schcompiler.AddResource(v.Id, strings.NewReader(string(jsonData)))
		if err != nil {
			log.Error("schcompiler.AddResource err: ", err)
			return nil, err
		}
	}
	for _, v := range sch.Vertices {
		sch, err := schcompiler.Compile(v.Id)
		if err != nil {
			log.Error("schcompiler.Compile err: ", err)
			return nil, err
		}
		out.Classes[v.Label] = sch
	}
	out.Compiler = schcompiler

	return out, nil
}

func GetSchemaPaths(sch *graph.GraphSchema) map[string][]string {
	retJsonPaths := make(map[string][]string, len(NodesMap))
	for _, cls := range sch.Classes {
		if _, isNode := NodesMap[cls.Title]; isNode {
			// Initialize the visited map for each class
			visited := make(map[string]struct{})
			if cls.Title != "" {
				visited[cls.Title] = struct{}{} // Mark the root schema as visited
			}
			retJsonPaths[cls.Title] = extractJSONPaths(cls, sch, "", visited)
		}
	}
	return retJsonPaths
}

func extractJSONPaths(schema *jsonschema.Schema, sch *graph.GraphSchema, prefix string, visited map[string]struct{}) []string {
	var paths []string

	if schema.Ref != nil && schema.Ref.Title != "" {
		// Check if this schema has already been visited
		if _, isVisited := visited[schema.Ref.Title]; isVisited {
			return paths // Stop recursion to prevent infinite loop
		}
		// Stop traversal if the reference is a schema class
		if _, isNode := NodesMap[schema.Ref.Title]; isNode {
			return paths // Separate entity, no further paths
		}
		// Mark this schema as visited
		visited[schema.Ref.Title] = struct{}{}
		// Traverse the referenced schema
		return extractJSONPaths(schema.Ref, sch, prefix, visited)
	}

	// Handle primitive types (leaf nodes)
	if len(schema.Types) > 0 && schema.Types[0] != "" && schema.Types[0] != "object" && schema.Types[0] != "array" {
		paths = append(paths, prefix)
		return paths
	}

	// Handle object properties
	if len(schema.Properties) > 0 {
		for key, property := range schema.Properties {
			// Skip fields like in GraphQL logic
			if key == "links" || key == "link" || (property.Ref != nil && property.Ref.Title != "" && slices.Contains([]string{"Reference", "FHIRPrimitiveExtension", "Extension"}, property.Ref.Title)) {
				continue
			}
			// Build the sub-prefix
			subPrefix := key
			if prefix != "" {
				subPrefix = prefix + "." + key
			}
			// Create a copy of the visited map for this branch
			subVisited := make(map[string]struct{}, len(visited))
			for k, v := range visited {
				subVisited[k] = v
			}
			// Recurse into the property
			subPaths := extractJSONPaths(property, sch, subPrefix, subVisited)
			paths = append(paths, subPaths...)
		}
		return paths
	}

	// Handle arrays
	if schema.Items2020 != nil {
		// Don't bother with indexing a specific part of the array. Select everything
		subPrefix := prefix + "[*]"
		if schema.Items2020.Ref != nil && schema.Items2020.Ref.Title != "" {
			// Check if this schema has already been visited
			if _, isVisited := visited[schema.Items2020.Ref.Title]; isVisited {
				return paths // Stop recursion to prevent infinite loop
			}
			if slices.Contains([]string{"Reference", "FHIRPrimitiveExtension", "Extension"}, schema.Items2020.Ref.Title) {
				return paths
			}
			// Stop if it’s a schema class
			if _, isNode := NodesMap[schema.Items2020.Ref.Title]; isNode {
				return paths
			}
			// Mark this schema as visited
			visited[schema.Items2020.Ref.Title] = struct{}{}
			return extractJSONPaths(schema.Items2020.Ref, sch, subPrefix, visited)
		}
		// Create a copy of the visited map for this branch
		subVisited := make(map[string]struct{}, len(visited))
		for k, v := range visited {
			subVisited[k] = v
		}
		// Recurse into array items
		return extractJSONPaths(schema.Items2020, sch, subPrefix, subVisited)
	}
	return paths
}
