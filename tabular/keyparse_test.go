package tabular

import (
  "testing"
)




func TestIndexKey(t *testing.T) {

  pathID := uint64(1)
  value := "Hello World"
  column := uint64(5)
  lineNum := uint64(950)

  key := IndexKey(pathID, column, value, lineNum)

  outPath, outColumn, outValue, outLine := IndexKeyParse(key)

  if pathID != outPath {
    t.Errorf("PathID incorrect %d != %d", pathID, outPath)
  }

  if column != outColumn {
    t.Errorf("Column incorrect %d != %d", column, outColumn)
  }

  if value != outValue {
    t.Errorf("Value incorrect %s != %s", value, outValue)
  }

  if lineNum != outLine {
    t.Errorf("Line incorrect %d != %d", lineNum, outLine)
  }

}
