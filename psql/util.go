package psql

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/bmeg/grip/gdbi"
)

type Row struct {
	Gid   string
	Label string
	From  string
	To    string
	Data  []byte
}

func ConvertVertexRow(row *Row, load bool) (*gdbi.Vertex, error) {
	props := make(map[string]interface{})
	if load {
		err := json.Unmarshal(row.Data, &props)
		if err != nil {
			return nil, fmt.Errorf("unmarshal error: %v", err)
		}
	}
	v := &gdbi.Vertex{
		ID:     row.Gid,
		Label:  row.Label,
		Data:   props,
		Loaded: load,
	}
	return v, nil
}

func ConvertEdgeRow(row *Row, load bool) (*gdbi.Edge, error) {
	props := make(map[string]interface{})
	if load {
		err := json.Unmarshal(row.Data, &props)
		if err != nil {
			return nil, fmt.Errorf("unmarshal error: %v", err)
		}
	}
	e := &gdbi.Edge{
		ID:     row.Gid,
		Label:  row.Label,
		From:   row.From,
		To:     row.To,
		Data:   props,
		Loaded: load,
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
