// Code generated for package gripqljs by go-bindata DO NOT EDIT. (@generated)
// sources:
// gripql.js
package gripqljs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)
type asset struct {
	bytes []byte
	info  os.FileInfo
}

type bindataFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
}

// Name return file name
func (fi bindataFileInfo) Name() string {
	return fi.name
}

// Size return file size
func (fi bindataFileInfo) Size() int64 {
	return fi.size
}

// Mode return file mode
func (fi bindataFileInfo) Mode() os.FileMode {
	return fi.mode
}

// Mode return file modify time
func (fi bindataFileInfo) ModTime() time.Time {
	return fi.modTime
}

// IsDir return file whether a directory
func (fi bindataFileInfo) IsDir() bool {
	return fi.mode&os.ModeDir != 0
}

// Sys return file is sys mode
func (fi bindataFileInfo) Sys() interface{} {
	return nil
}

var _gripqlJs = []byte(`function process(val) {
	if (!val) {
		val = []
  } else if (typeof val == "string" || typeof val == "number") {
	  val = [val]
  } else if (!Array.isArray(vall)) {
		throw "not something we know how to process into an array"
	}
	return val
}

function query() {
	return {
		query: [],
		V: function(id) {
			this.query.push({'v': process(id)})
			return this
		},
		E: function(id) {
			this.query.push({'e': process(id)})
			return this
		},
		out: function(label) {
			this.query.push({'out': process(label)})
			return this
		},
		in_: function(label) {
			this.query.push({'in': process(label)})
			return this
		},
		both: function(label) {
			this.query.push({'both': process(label)})
			return this
		},
		outV: function(label) {
			this.query.push({'outV': process(label)})
			return this
		},
		inV: function(label) {
			this.query.push({'inV': process(label)})
			return this
		},
		bothV: function(label) {
			this.query.push({'bothV': process(label)})
			return this
		},
		outE: function(label) {
			this.query.push({'out_e': process(label)})
			return this
		},
		inE: function(label) {
			this.query.push({'in_e': process(label)})
			return this
		},
		bothE: function(label) {
			this.query.push({'both_e': process(label)})
			return this
		},
		as_: function(name) {
			this.query.push({'as': name})
			return this
		},
		select: function(marks) {
			this.query.push({'select': {'marks': process(marks)}})
			return this
		},
		limit: function(n) {
			this.query.push({'limit': n})
			return this
		},
		skip: function(n) {
			this.query.push({'skip': n})
			return this
		},
		range: function(start, stop) {
			this.query.push({'range': {'start': start, 'stop': stop}})
			return this
		},
		count: function() {
			this.query.push({'count': ''})
			return this
		},
		distinct: function(val) {
			this.query.push({'distinct': process(val)})
			return this
		},
		fields: function(fields) {
			this.query.push({'fields': fields})
			return this
		},
		render: function(r) {
			this.query.push({'render': r})
			return this
		},
		has: function(expression) {
			this.query.push({'has': expression})
			return this
		},
		hasLabel: function(label) {
			this.query.push({'hasLabel': process(label)})
			return this
		},
		hasId: function(id) {
			this.query.push({'hasId': process(id)})
			return this
		},
		hasKey: function(key) {
			this.query.push({'hasKey': process(key)})
			return this
		},
		aggregate: function() {
			this.query.push({'aggregate': {'aggregations': Array.prototype.slice.call(arguments)}})
			return this
		}
	}
}

// Where operators
function and_() {
	return {'and': {'expressions': Array.prototype.slice.call(arguments)}}
}

function or_() {
	return {'or': {'expressions': Array.prototype.slice.call(arguments)}}
}

function not_(expression) {
	return {'not': expression}
}

function eq(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'EQ'}}
}

function neq(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'NEQ'}}
}

function gt(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'GT'}}
}

function gte(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'GTE'}}
}

function lt(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'LT'}}
}

function lte(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'LTE'}}
}

function inside(key, values) {
	return {'condition': {'key': key, 'value': process(values), 'condition': 'INSIDE'}}
}

function outside(key, values) {
	return {'condition': {'key': key, 'value': process(values), 'condition': 'OUTSIDE'}}
}

function between(key, values) {
	return {'condition': {'key': key, 'value': process(values), 'condition': 'BETWEEN'}}
}

function within(key, values) {
	return {'condition': {'key': key, 'value': process(values), 'condition': 'WITHIN'}}
}

function without(key, values) {
	return {'condition': {'key': key, 'value': process(values), 'condition': 'WITHOUT'}}
}

function contains(key, value) {
	return {'condition': {'key': key, 'value': value, 'condition': 'CONTAINS'}}
}

// Aggregation builders
function term(name, field, size) {
	agg = {
		"name": name,
		"term": {"field": field}
	}
	if (size) {
		if (typeof size != "number") {
			throw "expected size to be a number"
		}
		agg["term"]["size"] = size
	}
	return agg
}

function percentile(name, field, percents) {
	if (!percents) {
		percents = [1, 5, 25, 50, 75, 95, 99]
	} else {
		percents = process(percents)
	}

  if (!percents.every(function(x){ return typeof x == "number" })) {
		throw "percents expected to be an array of numbers"
	}

	return {
		"name": name,
		"percentile": {
			"field": field, "percents": percents
		}
	}
}

function histogram(name, field, interval) {
	if (interval) {
		if (typeof interval != "number") {
			throw "expected interval to be a number"
		}
	}
	return {
		"name": name,
		"histogram": {
			"field": field, "interval": interval
		}
	}
}

function V(id) {
  return query().V(id)
}

function E(id) {
  return query().E(id)
}
`)

func gripqlJsBytes() ([]byte, error) {
	return _gripqlJs, nil
}

func gripqlJs() (*asset, error) {
	bytes, err := gripqlJsBytes()
	if err != nil {
		return nil, err
	}

	info := bindataFileInfo{name: "gripql.js", size: 5083, mode: os.FileMode(420), modTime: time.Unix(1585610277, 0)}
	a := &asset{bytes: bytes, info: info}
	return a, nil
}

// Asset loads and returns the asset for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
func Asset(name string) ([]byte, error) {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	if f, ok := _bindata[cannonicalName]; ok {
		a, err := f()
		if err != nil {
			return nil, fmt.Errorf("Asset %s can't read by error: %v", name, err)
		}
		return a.bytes, nil
	}
	return nil, fmt.Errorf("Asset %s not found", name)
}

// MustAsset is like Asset but panics when Asset would return an error.
// It simplifies safe initialization of global variables.
func MustAsset(name string) []byte {
	a, err := Asset(name)
	if err != nil {
		panic("asset: Asset(" + name + "): " + err.Error())
	}

	return a
}

// AssetInfo loads and returns the asset info for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
func AssetInfo(name string) (os.FileInfo, error) {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	if f, ok := _bindata[cannonicalName]; ok {
		a, err := f()
		if err != nil {
			return nil, fmt.Errorf("AssetInfo %s can't read by error: %v", name, err)
		}
		return a.info, nil
	}
	return nil, fmt.Errorf("AssetInfo %s not found", name)
}

// AssetNames returns the names of the assets.
func AssetNames() []string {
	names := make([]string, 0, len(_bindata))
	for name := range _bindata {
		names = append(names, name)
	}
	return names
}

// _bindata is a table, holding each asset generator, mapped to its name.
var _bindata = map[string]func() (*asset, error){
	"gripql.js": gripqlJs,
}

// AssetDir returns the file names below a certain
// directory embedded in the file by go-bindata.
// For example if you run go-bindata on data/... and data contains the
// following hierarchy:
//     data/
//       foo.txt
//       img/
//         a.png
//         b.png
// then AssetDir("data") would return []string{"foo.txt", "img"}
// AssetDir("data/img") would return []string{"a.png", "b.png"}
// AssetDir("foo.txt") and AssetDir("notexist") would return an error
// AssetDir("") will return []string{"data"}.
func AssetDir(name string) ([]string, error) {
	node := _bintree
	if len(name) != 0 {
		cannonicalName := strings.Replace(name, "\\", "/", -1)
		pathList := strings.Split(cannonicalName, "/")
		for _, p := range pathList {
			node = node.Children[p]
			if node == nil {
				return nil, fmt.Errorf("Asset %s not found", name)
			}
		}
	}
	if node.Func != nil {
		return nil, fmt.Errorf("Asset %s not found", name)
	}
	rv := make([]string, 0, len(node.Children))
	for childName := range node.Children {
		rv = append(rv, childName)
	}
	return rv, nil
}

type bintree struct {
	Func     func() (*asset, error)
	Children map[string]*bintree
}

var _bintree = &bintree{nil, map[string]*bintree{
	"gripql.js": &bintree{gripqlJs, map[string]*bintree{}},
}}

// RestoreAsset restores an asset under the given directory
func RestoreAsset(dir, name string) error {
	data, err := Asset(name)
	if err != nil {
		return err
	}
	info, err := AssetInfo(name)
	if err != nil {
		return err
	}
	err = os.MkdirAll(_filePath(dir, filepath.Dir(name)), os.FileMode(0755))
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(_filePath(dir, name), data, info.Mode())
	if err != nil {
		return err
	}
	err = os.Chtimes(_filePath(dir, name), info.ModTime(), info.ModTime())
	if err != nil {
		return err
	}
	return nil
}

// RestoreAssets restores an asset under the given directory recursively
func RestoreAssets(dir, name string) error {
	children, err := AssetDir(name)
	// File
	if err != nil {
		return RestoreAsset(dir, name)
	}
	// Dir
	for _, child := range children {
		err = RestoreAssets(dir, filepath.Join(name, child))
		if err != nil {
			return err
		}
	}
	return nil
}

func _filePath(dir, name string) string {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	return filepath.Join(append([]string{dir}, strings.Split(cannonicalName, "/")...)...)
}
