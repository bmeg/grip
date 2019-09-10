package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/bmeg/grip/elastic"
	esql "github.com/bmeg/grip/existing-sql"
	"github.com/bmeg/grip/mongo"
	"github.com/bmeg/grip/psql"
	"github.com/bmeg/grip/server"
	"github.com/bmeg/grip/util"
	"github.com/bmeg/grip/util/duration"
	"github.com/bmeg/grip/util/rpc"
	"github.com/ghodss/yaml"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

// Config describes the configuration for Grip.
type Config struct {
	Database      string
	Server        server.Config
	RPCClient     rpc.Config
	KVStorePath   string
	Grids         string
	Elasticsearch elastic.Config
	MongoDB       mongo.Config
	PSQL          psql.Config
	ExistingSQL   esql.Config
	Logger        Logger
}

// DefaultConfig returns an instance of the default configuration for Grip.
func DefaultConfig() *Config {
	c := &Config{}
	c.Database = "badger"

	c.Server.HostName = "localhost"
	c.Server.HTTPPort = "8201"
	c.Server.RPCPort = "8202"
	c.Server.WorkDir = "grip.work"
	c.Server.ReadOnly = false
	c.Server.DisableHTTPCache = true
	c.Server.AutoBuildSchemas = false
	c.Server.SchemaRefreshInterval = duration.Duration(24 * time.Hour)
	c.Server.SchemaInspectN = 500
	c.Server.SchemaRandomSample = true

	c.RPCClient = rpc.ConfigWithDefaults(c.Server.RPCAddress())

	c.KVStorePath = "grip.db"

	c.MongoDB.DBName = "gripdb"
	c.MongoDB.BatchSize = 1000
	c.MongoDB.UseAggregationPipeline = true

	c.Elasticsearch.DBName = "gripdb"
	c.Elasticsearch.BatchSize = 1000

	c.Logger = DefaultLoggerConfig()
	return c
}

// TestifyConfig randomizes ports and database paths/names
func TestifyConfig(c *Config) {
	rand := strings.ToLower(util.RandomString(6))

	c.Server.HTTPPort = util.RandomPort()
	c.Server.RPCPort = util.RandomPort()
	c.Server.WorkDir = "grip.work." + rand

	c.RPCClient.ServerAddress = c.Server.RPCAddress()

	c.KVStorePath = "grip.db." + rand

	c.MongoDB.DBName = "gripdb-" + rand

	c.Elasticsearch.DBName = "gripdb-" + rand
	c.Elasticsearch.Synchronous = true
}

// ParseConfig parses a YAML doc into the given Config instance.
func ParseConfig(raw []byte, conf *Config) error {
	j, err := yaml.YAMLToJSON(raw)
	if err != nil {
		return err
	}
	err = CheckForUnknownKeys(j, conf)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(raw, conf)
	if err != nil {
		return err
	}
	return nil
}

// ParseConfigFile parses a config file, which is formatted in YAML,
// and returns a Config struct.
func ParseConfigFile(relpath string, conf *Config) error {
	if relpath == "" {
		return fmt.Errorf("config path is empty")
	}

	// Try to get absolute path. If it fails, fall back to relative path.
	path, err := filepath.Abs(relpath)
	if err != nil {
		path = relpath
	}

	// Read file
	source, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config at path %s: \n%v", path, err)
	}

	// Parse file
	err = ParseConfig(source, conf)
	if err != nil {
		return fmt.Errorf("failed to parse config at path %s: \n%v", path, err)
	}
	return nil
}

// GetKeys takes a struct or map and returns all keys that are present.
// Example:
// {"data": {"foo": "bar"}} => ["data", "data.foo"]
func GetKeys(obj interface{}) []string {
	keys := []string{}

	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			embedded := v.Type().Field(i).Anonymous
			name := v.Type().Field(i).Name
			keys = append(keys, name)

			valKeys := GetKeys(field.Interface())
			vk := []string{}
			for _, v := range valKeys {
				if embedded {
					vk = append(vk, v)
				}
				vk = append(vk, name+"."+v)
			}
			keys = append(keys, vk...)
		}
	case reflect.Map:
		for _, key := range v.MapKeys() {
			name := key.String()
			keys = append(keys, key.String())

			valKeys := GetKeys(v.MapIndex(key).Interface())
			for i, v := range valKeys {
				valKeys[i] = name + "." + v
			}
			keys = append(keys, valKeys...)
		}
	}
	return keys
}

// CheckForUnknownKeys takes a json byte array and checks that all keys are fields
// in the reference object
func CheckForUnknownKeys(jsonStr []byte, obj interface{}) error {
	knownMap := make(map[string]interface{})
	known := GetKeys(obj)
	for _, k := range known {
		knownMap[k] = nil
	}

	var anon interface{}
	err := json.Unmarshal(jsonStr, &anon)
	if err != nil {
		return err
	}

	unknown := []string{}
	all := GetKeys(anon)
	for _, k := range all {
		if _, found := knownMap[k]; !found {
			unknown = append(unknown, k)
		}
	}

	errs := []string{}
	if len(unknown) > 0 {
		for _, k := range unknown {
			parts := strings.Split(k, ".")
			field := parts[len(parts)-1]
			path := parts[:len(parts)-1]
			errs = append(
				errs,
				fmt.Sprintf("\t field %s not found in %s", field, strings.Join(path, ".")),
			)
		}
		return fmt.Errorf("%v", strings.Join(errs, "\n"))
	}

	return nil
}
