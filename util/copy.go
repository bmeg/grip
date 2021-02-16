package util



func DeepCopy(i interface{}) interface{} {
  if m, ok := i.(map[string]interface{}); ok {
    out := map[string]interface{}{}
    for k, v := range m {
      out[k] = DeepCopy(v)
    }
    return out
  } else if a, ok := i.([]interface{}); ok {
    out := make([]interface{}, len(a))
    for k, v := range a {
      out[k] = DeepCopy(v)
    }
    return out
  }
  return i
}
