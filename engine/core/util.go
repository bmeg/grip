package core

import (
	"github.com/kr/pretty"
)

func debug(i ...interface{}) {
	pretty.Println(i...)
}

func contains(a []string, v string) bool {
	for _, i := range a {
		if i == v {
			return true
		}
	}
	return false
}
