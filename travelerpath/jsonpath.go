package travelerpath

import (
	"strings"

	"github.com/bmeg/grip/gripql"
)

// Current represents the 'current' traveler namespace
var Current = "__current__"

// GetNamespace returns the namespace of the provided path
//
// Example:
// GetNamespace("$gene.symbol.ensembl") returns "gene"
func GetNamespace(path string) string {
	namespace := ""
	parts := strings.Split(path, ".")
	if strings.HasPrefix(parts[0], "$") {
		namespace = strings.TrimPrefix(parts[0], "$")
	}
	if namespace == "" {
		namespace = Current
	}
	return namespace
}

// GetJSONPath strips the namespace from the path and returns the valid
// Json path within the document referenced by the namespace
//
// Example:
// GetJSONPath("gene.symbol.ensembl") returns "$.data.symbol.ensembl"
func GetJSONPath(path string) string {
	parts := strings.Split(path, ".")
	if strings.HasPrefix(parts[0], "$") {
		parts = parts[1:]
	}
	if len(parts) == 0 {
		return ""
	}
	found := false
	for _, v := range gripql.ReservedFields {
		if parts[0] == v {
			found = true
			parts[0] = strings.TrimPrefix(parts[0], "_")
		}
	}

	if !found {
		parts = append([]string{"data"}, parts...)
	}

	parts = append([]string{"$"}, parts...)
	return strings.Join(parts, ".")
}
