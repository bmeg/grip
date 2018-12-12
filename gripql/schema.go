package gripql

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/ghodss/yaml"
	"github.com/golang/protobuf/jsonpb"
)

// ParseSchema parses a YAML doc into the given GraphSchema instance.
func ParseSchema(raw []byte) ([]*GraphSchema, error) {
	schemas := []*GraphSchema{}
	tmp := []interface{}{}
	err := yaml.Unmarshal(raw, &tmp)
	if err != nil {
		return nil, err
	}
	for _, s := range tmp {
		part, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		schema := &GraphSchema{}
		err = jsonpb.UnmarshalString(string(part), schema)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}
	return schemas, nil
}

// ParseSchemaFile parses a graph schema file, which is formatted in YAML,
// and returns a slice of graph schemas.
func ParseSchemaFile(relpath string) ([]*GraphSchema, error) {
	if relpath == "" {
		return nil, fmt.Errorf("schema path is empty")
	}

	// Try to get absolute path. If it fails, fall back to relative path.
	path, err := filepath.Abs(relpath)
	if err != nil {
		path = relpath
	}

	// Read file
	source, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema at path %s: \n%v", path, err)
	}

	// Parse file
	schemas, err := ParseSchema(source)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema at path %s: \n%v", path, err)
	}

	return schemas, nil
}

// GetDataFieldTypes iterates over the data map and determines the type of each field
func GetDataFieldTypes(data map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{})
	for key, val := range data {
		if vMap, ok := val.(map[string]interface{}); ok {
			out[key] = GetDataFieldTypes(vMap)
			continue
		}
		if vSlice, ok := val.([]interface{}); ok {
			var vType interface{}
			vType = []interface{}{FieldType_UNKNOWN.String()}
			if len(vSlice) > 0 {
				vSliceVal := vSlice[0]
				if vSliceValMap, ok := vSliceVal.(map[string]interface{}); ok {
					vType = []map[string]interface{}{GetDataFieldTypes(vSliceValMap)}
				} else {
					vType = []interface{}{GetFieldType(vSliceVal)}
				}
			}
			out[key] = vType
			continue
		}
		out[key] = GetFieldType(val)
	}
	return out
}

// GetFieldType returns the FieldType for a value
func GetFieldType(field interface{}) string {
	switch field.(type) {
	case string:
		return FieldType_STRING.String()
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return FieldType_NUMERIC.String()
	case float32, float64:
		return FieldType_NUMERIC.String()
	case bool:
		return FieldType_BOOL.String()
	default:
		return FieldType_UNKNOWN.String()
	}
}
