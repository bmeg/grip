package arango

import (
	"fmt"
	"strings"
	"text/template"
)

var funcMap = template.FuncMap{
	"join": func(sep string, items []string) string {
		return strings.Join(items, sep)
	},
}

type ASTBase struct {
	ForLoop *ForLoop
}

type Statement interface {
	SetIndent(indent string)
	String() string
}

type ForLoop struct {
	Indent     string
	Variables  []string
	Collection string
	Range      string // e.g., "1..1"
	Direction  string // "OUTBOUND", "INBOUND", "ANY"
	Source     string // source vertex, e.g., "v0"
	GraphName  string // graph name for graph traversals
	Body       ForStatement
}

type ForStatement struct {
	Indent   string
	Children []Statement // *FilterStatement, *ForLoop, *ReturnStatement
}

func (fs *ForStatement) addChild(child Statement) {
	fs.Children = append(fs.Children, child)
}

type FilterStatement struct {
	Indent string
	Expr   string
}

type ReturnStatement struct {
	Indent   string
	Variable string
}

type SortStatement struct {
	Indent string
	Expr   string
}

type LimitStatement struct {
	Indent string
	Limit  int
}

func (b *ASTBase) String() string {
	if b.ForLoop != nil {
		return "\n" + b.ForLoop.String()
	}
	return ""
}

// joinStrings joins a slice with the given separator
func joinStrings(items []string, sep string) string {
	result := ""
	for i, item := range items {
		if i > 0 {
			result += sep
		}
		result += item
	}
	return result
}

// addIndent returns a string with the given number of indentation levels (2 spaces each)
func addIndent(level int) string {
	return strings.Repeat("  ", level)
}

func (f *ForLoop) String() string {
	indent := f.Indent
	var line string
	if f.Range != "" && f.Direction != "" && f.Source != "" && f.GraphName != "" {
		line = fmt.Sprintf("%sFOR %s IN %s %s %s %s", indent, joinStrings(f.Variables, ", "), f.Range, f.Direction, f.Source, f.GraphName)
	} else if f.Collection != "" {
		line = fmt.Sprintf("%sFOR %s IN %s", indent, joinStrings(f.Variables, ", "), f.Collection)
	}

	result := line + "\n" + f.Body.String(len(indent)/2+1)
	return result
}

func (f *ForStatement) String(indentLevel int) string {
	out := ""
	indent := addIndent(indentLevel)
	for _, child := range f.Children {
		child.SetIndent(indent)
		out += child.String()
	}
	return out
}

func (f *ForLoop) SetIndent(indent string) {
	f.Indent = indent
}

func (fs *ForStatement) SetIndent(indent string) {
	fs.Indent = indent
	for _, child := range fs.Children {
		child.SetIndent(indent + "  ")
	}
}

func (f *FilterStatement) SetIndent(indent string) {
	f.Indent = indent
}

func (r *ReturnStatement) SetIndent(indent string) {
	r.Indent = indent
}

func (f *FilterStatement) String() string {
	return fmt.Sprintf("%sFILTER %s\n", f.Indent, f.Expr)
}

func (r *ReturnStatement) String() string {
	return fmt.Sprintf("%sRETURN %s\n", r.Indent, r.Variable)
}

func (s *SortStatement) SetIndent(indent string) {
	s.Indent = indent
}

func (s *SortStatement) String() string {
	return fmt.Sprintf("%sSORT %s\n", s.Indent, s.Expr)
}

func (l *LimitStatement) SetIndent(indent string) {
	l.Indent = indent
}

func (l *LimitStatement) String() string {
	return fmt.Sprintf("%sLIMIT %d\n", l.Indent, l.Limit)
}
