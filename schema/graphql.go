package schema

import (
	"fmt"
	"os"
	"strings"
	"unicode"

	"github.com/bmeg/grip/gripql"
)

func IsUpper(s string) bool {
	for _, r := range s {
		if !unicode.IsUpper(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

func GripGraphqltoGraphql(graph *gripql.Graph, writefile bool) string {
	var schemaBuilder strings.Builder
	// Write gen3 style boiler plate to mirror thier args
	schemaBuilder.WriteString("scalar JSON\n")
	schemaBuilder.WriteString("enum Accessibility {\n  all\n  accessible\n  unaccessible\n}\n")
	schemaBuilder.WriteString("enum Format {\n  json\n  tsv\n  csv\n}\n")

	for _, v := range graph.Vertices {
		if v.Gid != "Query" {
			executedFirstBlock := false
			for name, values := range v.Data.AsMap() {
				listVals, ok := values.([]any)
				if ok && IsUpper(listVals[0].(string)) {
					executedFirstBlock = true
					schemaBuilder.WriteString(fmt.Sprintf("enum %s {\n", name))
					for _, value := range listVals {
						schemaBuilder.WriteString(fmt.Sprintf("  %s\n", value))
					}
					schemaBuilder.WriteString("}\n")
				} else {
					break
				}
			}
			if !executedFirstBlock {
				schemaBuilder.WriteString(fmt.Sprintf("type %s {\n", v.Gid))
				for field, fieldType := range v.Data.AsMap() {
					schemaBuilder.WriteString(fmt.Sprintf("  %s: %s\n", field, fieldType))
				}
				schemaBuilder.WriteString("}\n")
			}
		} else {
			for name, values := range v.Data.AsMap() {
				schemaBuilder.WriteString(fmt.Sprintf("type %s {\n", name))
				for _, value := range values.([]any) {
					schemaBuilder.WriteString(fmt.Sprintf("  %s\n", value))
				}
				schemaBuilder.WriteString("}\n")
			}
		}
	}
	if writefile {
		fileName := "schema.graphql"
		err := os.WriteFile(fileName, []byte(schemaBuilder.String()), 0644)
		if err != nil {
			fmt.Printf("Failed to write schema to file: %v\n", err)
		}
	}
	return schemaBuilder.String()
}
