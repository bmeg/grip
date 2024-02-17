package mongo

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bmeg/grip/gdbi/tpath"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util"
	"go.mongodb.org/mongo-driver/bson"
)

func TestQuerySizeLimit(t *testing.T) {
	Q := &gripql.Query{}
	ids := []string{}
	i := 0
	for i < 1000000 {
		ids = append(ids, util.UUID())
		i++
	}
	Q = Q.V(ids...)

	c := NewCompiler(&Graph{})
	_, err := c.Compile(Q.Statements, nil)
	t.Log(err)
	if err == nil {
		t.Error("expected an error on compile")
	}
}

func TestDistinctPathing(t *testing.T) {

	fields := []string{"$case._gid", "$compound._gid"}

	match := bson.M{}
	keys := bson.M{}

	for _, f := range fields {
		namespace := tpath.GetNamespace(f)
		fmt.Printf("Namespace: %s\n", namespace)
		f = tpath.NormalizePath(f)
		f = strings.TrimPrefix(f, "$.")
		if f == "gid" {
			f = FIELD_ID
		}
		if namespace != tpath.CURRENT {
			f = fmt.Sprintf("marks.%s.%s", namespace, f)
		}
		match[f] = bson.M{"$exists": true}
		k := strings.Replace(f, ".", "_", -1)
		keys[k] = "$" + f
	}
	if m, ok := match["marks.case._id"]; ok {
		m1 := m.(bson.M)
		if e, ok := m1["$exists"]; ok {
			if b, ok := e.(bool); ok {
				if !b {
					t.Errorf("$exist value incorrect")
				}
			} else {
				t.Errorf("$exist value incorrect")
			}
		} else {
			t.Errorf("Mark key not formatted correctly")
		}
	} else {
		t.Errorf("mark key not found")
	}
}
