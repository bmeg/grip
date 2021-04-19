package jobstorage

import (
	"fmt"

	"github.com/bmeg/grip/gripql"
	"github.com/mitchellh/hashstructure/v2"
)

func QueryChecksum(query []*gripql.GraphStatement) (string, error) {
	hash, err := hashstructure.Hash(query, hashstructure.FormatV2, nil)
	out := fmt.Sprintf("%d", hash)
	return out, err
}

func TraversalChecksum(query []*gripql.GraphStatement) ([]string, error) {
	out := make([]string, len(query))
	for i := range query {
		hash, err := hashstructure.Hash(query[i], hashstructure.FormatV2, nil)
		out[i] = fmt.Sprintf("%d", hash)
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

func JobMatch(query []string, job []string) bool {
	if len(job) > len(query) {
		return false
	}
	match := true
	for i := 0; i < len(job); i++ {
		if query[i] != job[i] {
			match = false
		}
	}
	return len(job) > 1 && match
}
