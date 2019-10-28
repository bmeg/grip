package test

import (
  "os"
  "fmt"
  "testing"
  "github.com/akrylysov/pogreb"
  "github.com/bmeg/grip/grids"
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

  keymap := grids.NewKeyMap(keykv)

  graphKey := keymap.GetGraphKey("test")

  vertexKeys := make([]uint64, 100)
  var evenLabel uint64
  for i := range vertexKeys {
    label := "even"
    if i % 2 == 1 {
      label = "odd"
    }
    k, l := keymap.GetsertVertexKey(graphKey, fmt.Sprintf("vertex_%d", i), label)
    if i == 0 {
      evenLabel = l
    } else {
      if i % 2 == 1 {
        if evenLabel == l {
          t.Errorf("Getsert returns wrong key")
        }
      } else {
        if evenLabel != l {
          t.Errorf("Getsert returns wrong key")
        }
      }
    }
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
    id := keymap.GetVertexID(graphKey, vertexKeys[i])
    if id != fmt.Sprintf("vertex_%d", i) {
      t.Errorf("ID test_%d != %s", i, id)
    }
    lkey := keymap.GetVertexLabel(graphKey, vertexKeys[i])
    lid := keymap.GetLabelID(graphKey, lkey)
    if i % 2 == 1 {
      if lid != "odd" {
        t.Errorf("Wrong vertex label %s : %s != %s", id, lid, "odd")
      }
    } else {
      if lid != "even" {
        t.Errorf("Wrong vertex label %s : %s != %s", id, lid, "even")
      }
    }
  }


  edgeKeys := make([]uint64, 100)
  for i := range edgeKeys {
    label := "even_edge"
    if i % 2 == 1 {
      label = "odd_edge"
    }
    k, _ := keymap.GetsertEdgeKey(graphKey, fmt.Sprintf("edge_%d", i), label)
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
    id := keymap.GetEdgeID(graphKey, edgeKeys[i])
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
    k := keymap.GetsertLabelKey(graphKey, fmt.Sprintf("label_%d", i))
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
    id := keymap.GetLabelID(graphKey, labelKeys[i])
    if id != fmt.Sprintf("label_%d", i) {
      t.Errorf("ID graph_%d != %s", i, id)
    }
  }

  dbClose(keykv)
}
