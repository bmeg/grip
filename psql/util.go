package psql

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/protoutil"
)

type row struct {
	Gid   string
	Label string
	From  string
	To    string
	Data  []byte
}

func convertVertexRow(row *row, load bool) (*gripql.Vertex, error) {
	props := make(map[string]interface{})
	if load {
		err := json.Unmarshal(row.Data, &props)
		if err != nil {
			return nil, fmt.Errorf("unmarshal error: %v", err)
		}
	}
	v := &gripql.Vertex{
		Gid:   row.Gid,
		Label: row.Label,
		Data:  protoutil.AsStruct(props),
	}
	return v, nil
}

func convertEdgeRow(row *row, load bool) (*gripql.Edge, error) {
	props := make(map[string]interface{})
	if load {
		err := json.Unmarshal(row.Data, &props)
		if err != nil {
			return nil, fmt.Errorf("unmarshal error: %v", err)
		}
	}
	e := &gripql.Edge{
		Gid:   row.Gid,
		Label: row.Label,
		From:  row.From,
		To:    row.To,
		Data:  protoutil.AsStruct(props),
	}
	return e, nil
}

func dbDoesNotExist(err error) bool {
	matched, err := regexp.MatchString(`database "[a-z_]+" does not exist`, err.Error())
	if err != nil {
		return false
	}
	return matched
}
