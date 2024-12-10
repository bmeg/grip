package schema

import (
	"fmt"
	"os"
	"strings"

	"github.com/bmeg/grip/gripql"
)

func GripGraphqltoGraphql(graph *gripql.Graph, writefile bool) string {
	var schemaBuilder strings.Builder
	for _, v := range graph.Vertices {
		if strings.HasSuffix(v.Gid, "Type") {
			for name, values := range v.Data.AsMap() {
				schemaBuilder.WriteString(fmt.Sprintf("enum %s {\n", name))
				for _, value := range values.([]any) {
					schemaBuilder.WriteString(fmt.Sprintf("  %s\n", value))
				}
				schemaBuilder.WriteString("}\n\n")
			}
		} else {
			schemaBuilder.WriteString(fmt.Sprintf("type %s {\n", v.Gid))
			for field, fieldType := range v.Data.AsMap() {
				schemaBuilder.WriteString(fmt.Sprintf("  %s: %s\n", field, fieldType))
			}
			schemaBuilder.WriteString("}\n")
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
