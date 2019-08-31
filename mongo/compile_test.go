package mongo

import (
	"fmt"
	"testing"
	"strings"

	"github.com/bmeg/grip/jsonpath"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/util"
	"github.com/globalsign/mgo/bson"

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
	_, err := c.Compile(Q.Statements)
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
		namespace := jsonpath.GetNamespace(f)
		fmt.Printf("Namespace: %s\n", namespace)
		f = jsonpath.GetJSONPath(f)
		f = strings.TrimPrefix(f, "$.")
		if f == "gid" {
			f = "_id"
		}
		if namespace != jsonpath.Current {
			f = fmt.Sprintf("marks.%s.%s", namespace, f)
		}
		match[f] = bson.M{"$exists": true}
		k := strings.Replace(f, ".", "_", -1)
		keys[k] = "$" + f
	}
	fmt.Printf("%s\n", match)
	fmt.Printf("%s\n", keys)
}
