package gen3

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/protoutil"
	//log "github.com/sirupsen/logrus"
)

type row struct {
	NodeID string `db:"node_id"`
	SrcID  string `db:"src_id"`
	DstID  string `db:"dst_id"`
	Props  []byte `db:"_props"`
}

func convertVertexRow(row *row, label string, load bool) (*gripql.Vertex, error) {
	props := make(map[string]interface{})
	if load && len(row.Props) > 0 {
		err := json.Unmarshal(row.Props, &props)
		if err != nil {
			return nil, fmt.Errorf("unmarshal error: %v", err)
		}
	}
	v := &gripql.Vertex{
		Gid:   row.NodeID,
		Label: label,
		Data:  protoutil.AsStruct(props),
	}
	return v, nil
}

func convertEdgeRow(row *row, label string, load bool) (*gripql.Edge, error) {
	props := make(map[string]interface{})
	if load && len(row.Props) > 0 {
		err := json.Unmarshal(row.Props, &props)
		if err != nil {
			return nil, fmt.Errorf("unmarshal error: %v", err)
		}
	}
	e := &gripql.Edge{
		Gid:   fmt.Sprintf("%s_%s", row.SrcID, row.DstID),
		Label: label,
		From:  row.SrcID,
		To:    row.DstID,
		Data:  protoutil.AsStruct(props),
	}
	return e, nil
}

func getEdgeIDParts(gid string) (srcID string, dstID string) {
	srcID = ""
	dstID = ""
	parts := strings.SplitN(gid, "_", 2)
	if len(parts) != 2 {
		return
	}
	srcID = parts[0]
	dstID = parts[1]
	return
}

func noRowsInResult(err error) bool {
	return strings.Contains(err.Error(), "no rows in result set")
}

func tableDoesNotExist(err error) bool {
	matched, err := regexp.MatchString(`relation "[a-z_]+" does not exist`, err.Error())
	if err != nil {
		return false
	}
	return matched
}

func dbDoesNotExist(err error) bool {
	matched, err := regexp.MatchString(`database "[a-z_]+" does not exist`, err.Error())
	if err != nil {
		return false
	}
	return matched
}
