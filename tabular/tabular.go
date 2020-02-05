package tabular

import (
  "fmt"
  "context"
  "github.com/bmeg/grip/kvi"
  "github.com/bmeg/grip/kvi/badgerdb"
)


type TableManager struct {
  Index  TableIndex
}

type TableRow struct {
  Key    string
  Values map[string]string
}

type Driver interface {
  GetIDs(ctx context.Context) chan string
  GetRows(ctx context.Context) chan *TableRow
  GetRowByID(id string) (*TableRow, error)
  //GetRowsByField(ctx context.Context, field string, value string) (chan *TableRow, error)
}

type Options struct {
  PrimaryKey      string
  IndexedColumns  []string
}

type DriverBuilder func(url string, manager *TableManager, opts Options) (Driver, error)

var driverMap = make(map[string]DriverBuilder)

// AddDriver registers a tabular driver to the list of avalible drivers.
func AddDriver(name string, builder DriverBuilder) error {
	driverMap[name] = builder
	return nil
}

// NewDriver intitalize a new key value interface given the name of the
// driver and path to create the database
func (t *TableManager) NewDriver(name string, url string, opts Options) (Driver, error) {
	if builder, ok := driverMap[name]; ok {
		return builder(url, t, opts)
	}
	return nil, fmt.Errorf("driver %s Not Found", name)
}


func NewTableManager(path string) (*TableManager, error) {
  out := TableManager{}
  //kv, err := boltdb.NewKVInterface(path, kvi.Options{})
  kv, err := badgerdb.NewKVInterface(path, kvi.Options{})
  if err != nil {
    return nil, err
  }
  out.Index = TableIndex{kv}
  return &out, nil
}

/*
func (t *TableManager) NewDriver(driver string, url string, idName string, idxCols []string) *TableManager {
  o := TSVIndex{kv:t.kv, path:path, idName:idName, idxCols:idxCols, cparse:CSVParse{}}
  o.Init()
  return &o
}

func (t *TabularIndex) Close() error {
    return t.kv.Close()
}
*/
