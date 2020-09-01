package gripper

import (
  "fmt"
  "strings"
)


func (t *TabularGraph) ParseEdge(gid string) (string, string, string, error) {
  tmp := strings.Split(gid, "-")
  if len(tmp) != 3 {
    return "", "", "", fmt.Errorf("Incorrectly formatted GID")
  }
  return tmp[0], tmp[2], tmp[1], nil
}


func (es *EdgeSource) GenID(srcID, dstID string) string {
  if es.reverse {
    return es.toVertex.prefix + dstID + "-" + es.config.Label + "-" + es.fromVertex.prefix + srcID
  }
  return es.fromVertex.prefix + srcID + "-" + es.config.Label + "-" + es.toVertex.prefix + dstID
}
