package core

import (
	"github.com/kr/pretty"
)

func debug(i ...any) {
	pretty.Println(i...)
}

func dedupStringSlice(s []string) []string {
	seen := make(map[string]struct{}, len(s))
	j := 0
	for _, v := range s {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		s[j] = v
		j++
	}
	return s[:j]
}
