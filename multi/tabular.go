package multi

import (
  "fmt"
  "context"
  "strings"
  "github.com/oliveagle/jsonpath"
)


type TableManager struct {
  Index  Cache
}

type TableRow struct {
  Key    string
  Values map[string]interface{}
}

//Driver Primay Interface for table based graph
type Driver interface {
  GetIDs(ctx context.Context) chan string
  GetRows(ctx context.Context) chan *TableRow
  GetRowByID(id string) (*TableRow, error)
  GetRowsByField(ctx context.Context, field string, value string) chan *TableRow
}

//LineIndex Cached index for line offsets
type LineIndex interface {
  GetIDLine(id string) (uint64, error)
  GetIDChannel(ctx context.Context) chan string
  GetLineCount() (uint64, error)
  GetLineOffset(line uint64 ) (uint64, error)
  AddIndexedField(col string)
  GetLinesByField(ctx context.Context, field string, value string) chan uint64
  IndexWrite( f func(LineIndexWriter) error )
}

//LineIndexWriter
type LineIndexWriter interface {
  SetIDLine( id string, line uint64)
  SetLineOffset( line uint64, offset uint64)
  SetLineCount( lineCount uint64)
  IndexRow( line uint64, row map[string]interface{}) error
}

type RowStorage interface {
  Write(row *TableRow) error
  GetRowByID(id string) (*TableRow, error)
  GetRowsByField(ctx context.Context, field string, value string) chan *TableRow
}

//Cache
type Cache interface {
  NewLineIndex(path string) (LineIndex, error)
  GetLineIndex(path string) (LineIndex, error)

  NewRowStorage(path string) (RowStorage, error)
  GetRowStorage(path string) (RowStorage, error)
}

type Options struct {
  PrimaryKey      string
  IndexedColumns  []string
  Config          map[string]interface{}
}

type DriverBuilder func(name, url string, cache Cache, opts Options) (Driver, error)
type CacheBuilder func(url string) (Cache, error)

var driverMap = make(map[string]DriverBuilder)
var cacheMap = make(map[string]CacheBuilder)

// AddDriver registers a tabular driver to the list of avalible drivers.
func AddDriver(name string, builder DriverBuilder) error {
	driverMap[name] = builder
	return nil
}

func AddCache(name string, builder CacheBuilder) error {
  cacheMap[name] = builder
  return nil
}

// NewDriver intitalize a new key value interface given the name of the
// driver and path to create the database
func (t *TableManager) NewDriver(name string, driver string, url string, opts Options) (Driver, error) {
	if builder, ok := driverMap[driver]; ok {
		return builder(name, url, t.Index, opts)
	}
	return nil, fmt.Errorf("driver %s Not Found", name)
}


func NewCache(path string) (Cache, error) {
  return cacheMap["kv"](path)
}


func pathFix(p string) string {
  if !strings.HasPrefix(p, "$.") {
    return "$." + p
  }
  return p
}

func FieldFilter(field string, value string, data map[string]interface{}) bool {
  if field == "" {
    return true
  }
  v, err := jsonpath.JsonPathLookup(data, pathFix(field) )
  if err != nil {
    return false
  }
  if valStr, ok := v.(string); ok {
    if valStr == value {
      return true
    }
  }
  return false
}
