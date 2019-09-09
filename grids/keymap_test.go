package grids

import (
  "os"
  "fmt"
  "testing"
  "github.com/akrylysov/pogreb"
)

var kvPath string = "test.db"


func dbSetup() (*pogreb.DB, error) {
  os.Mkdir("test_db", 0700)
  return pogreb.Open("test_db/data", nil)
}

func dbClose(db *pogreb.DB) {
  db.Close()
  os.RemoveAll("test_db")
}

func TestKeyInsert(t *testing.T) {
  keykv, err := dbSetup()
	if err != nil {
    t.Error(err)
	}

  keymap := NewKeyMap(keykv)

  vertexKeys := make([]uint64, 100)
  for i := range vertexKeys {
    k := keymap.GetVertexKey(fmt.Sprintf("vertex_%d", i))
    vertexKeys[i] = k
  }
  for i := range vertexKeys {
    for j := range vertexKeys {
      if i != j {
        if vertexKeys[i] == vertexKeys[j] {
          t.Errorf("Non unique keys: %d %d", vertexKeys[i], vertexKeys[j])
        }
      }
    }
  }
  for i := range vertexKeys {
    id := keymap.GetVertexID(vertexKeys[i])
    if id != fmt.Sprintf("vertex_%d", i) {
      t.Errorf("ID test_%d != %s", i, id)
    }
  }


  edgeKeys := make([]uint64, 100)
  for i := range edgeKeys {
    k := keymap.GetEdgeKey(fmt.Sprintf("edge_%d", i))
    edgeKeys[i] = k
  }
  for i := range edgeKeys {
    for j := range edgeKeys {
      if i != j {
        if edgeKeys[i] == edgeKeys[j] {
          t.Errorf("Non unique keys: %d %d", edgeKeys[i], edgeKeys[j])
        }
      }
    }
  }
  for i := range edgeKeys {
    id := keymap.GetEdgeID(edgeKeys[i])
    if id != fmt.Sprintf("edge_%d", i) {
      t.Errorf("ID test_%d != %s", i, id)
    }
  }


  graphKeys := make([]uint64, 100)
  for i := range graphKeys {
    k := keymap.GetGraphKey(fmt.Sprintf("graph_%d", i))
    graphKeys[i] = k
  }
  for i := range graphKeys {
    for j := range graphKeys {
      if i != j {
        if graphKeys[i] == graphKeys[j] {
          t.Errorf("Non unique keys: %d %d", graphKeys[i], graphKeys[j])
        }
      }
    }
  }
  for i := range graphKeys {
    id := keymap.GetGraphID(graphKeys[i])
    if id != fmt.Sprintf("graph_%d", i) {
      t.Errorf("ID graph_%d != %s", i, id)
    }
  }

  labelKeys := make([]uint64, 100)
  for i := range labelKeys {
    k := keymap.GetLabelKey(fmt.Sprintf("label_%d", i))
    labelKeys[i] = k
  }
  for i := range labelKeys {
    for j := range labelKeys {
      if i != j {
        if labelKeys[i] == labelKeys[j] {
          t.Errorf("Non unique keys: %d %d", labelKeys[i], labelKeys[j])
        }
      }
    }
  }
  for i := range labelKeys {
    id := keymap.GetLabelID(labelKeys[i])
    if id != fmt.Sprintf("label_%d", i) {
      t.Errorf("ID graph_%d != %s", i, id)
    }
  }

  dbClose(keykv)
}
