package tpath

import (
	"strings"
)

// Current represents the 'current' traveler namespace
const CURRENT = "_current"

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
		namespace = CURRENT
	}
	return namespace
}

// NormalizePath
//
// Example:
// NormalizePath("gene.symbol.ensembl") returns "$_current.symbol.ensembl"

func NormalizePath(path string) string {
	namespace := CURRENT
	parts := strings.Split(path, ".")

	if strings.HasPrefix(parts[0], "$") {
		if len(parts[0]) > 1 {
			namespace = parts[0][1:]
		}
		parts = parts[1:]
	}

	parts = append([]string{"$" + namespace}, parts...)
	return strings.Join(parts, ".")
}

func ToLocalPath(path string) string {
	parts := strings.Split(path, ".")
	if strings.HasPrefix(parts[0], "$") {
		parts[0] = "$"
	}
	return strings.Join(parts, ".")
}

func distinct(x []string) []string {
	c := map[string]bool{}
	for _, k := range x {
		c[k] = true
	}
	out := []string{}
	for k := range c {
		out = append(out, k)
	}
	return out
}

func GetAllNamespaces(d any) []string {
	out := []string{}

	if x, ok := d.([]any); ok {
		for _, c := range x {
			l := GetAllNamespaces(c)
			if len(l) > 0 {
				out = append(out, l...)
			}
		}
		return distinct(out)
	} else if x, ok := d.(map[string]any); ok {
		for k, v := range x {
			if strings.HasPrefix(k, "$") {
				out = append(out, GetNamespace(k))
			}
			l := GetAllNamespaces(v)
			if len(l) > 0 {
				out = append(out, l...)
			}
		}
		return distinct(out)
	} else if x, ok := d.(string); ok {
		if strings.HasPrefix(x, "$") {
			out = append(out, GetNamespace(x))
		}
	}
	return out
}
