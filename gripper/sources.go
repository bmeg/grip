package gripper



func (es *EdgeSource) GenID(srcID, dstID string) string {
  if es.reverse {
    return es.toVertex.prefix + dstID + "-" + es.config.Label + "-" + es.fromVertex.prefix + srcID
  }
  return es.fromVertex.prefix + srcID + "-" + es.config.Label + "-" + es.toVertex.prefix + dstID
}
