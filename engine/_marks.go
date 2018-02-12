
type markKey int

type marks struct {
  d map[markKey]*Element
  k markKey
  keys map[string]markKey
  mtx sync.RWMutex
}

func newMarks() *marks {
  return &marks{
    d: map[markKey]*Element{},
    keys: map[string]markKey,
  }
}

func (m *marks) newKeys(names []string) []markKey {
  m.mtx.Lock()
  defer m.mtx.Unlock()
  keys := make([]markKeys, len(names))
  for _, name := range names {
    k := m.k
    m.keys[name] = k
    m.k++
    keys = append(keys, k)
  }
  return keys
}

func (m *marks) getKeys(names []string) (keys []markKey, missing []string) {
  m.mtx.RLock()
  defer m.mtx.RUnlock()
  keys = make([]markKeys, len(names))
  for _, name := range names {
    key, ok := m.keys[name]
    if !ok {
      missing = append(missing, name)
    } else {
      keys = append(keys, key)
    }
  }
  return keys, missing
}

func (m *marks) setMarks(keys []markKey, el *Element) {
  m.mtx.Lock()
  defer m.mtx.Unlock()
  for _, key := range keys {
    m.d[key] = el
  }
}

func (m *marks) unsetMarks(key []markKey) {
  m.mtx.Lock()
  defer m.mtx.Unlock()
  for _, key := range keys {
    delete(m.d, key)
  }
}

func (m *marks) getMarks(keys []markKey) []*Element {
  m.mtx.RLock()
  defer m.mtx.RUnlock()

  var out []*Element
  for _, key := range keys {
    // TODO what if name is not found? can code handle a nil Element?
    //      pipe could drop nils sent to emit(). this would mimic gremlin?
    out = append(out, m.d[key])
  }
  return out
}
