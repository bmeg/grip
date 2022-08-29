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
	"github.com/bmeg/grip/gripper"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/mongo"
	"github.com/bmeg/grip/psql"
	"github.com/bmeg/grip/util"
	"github.com/bmeg/grip/util/duration"
	"github.com/bmeg/grip/util/rpc"
	"sigs.k8s.io/yaml"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

type DriverConfig struct {
	Grids         *string
	Badger        *string
	Bolt          *string
	Level         *string
	Pebble        *string
	Elasticsearch *elastic.Config
	MongoDB       *mongo.Config
	PSQL          *psql.Config
	ExistingSQL   *esql.Config
	Gripper       *gripper.Config
}

// Config describes the configuration for Grip.
type Config struct {
	Server    ServerConfig
	RPCClient rpc.Config
	Logger    log.Logger
	Default   string
	Graphs    map[string]string
	Drivers   map[string]DriverConfig
	Sources   map[string]string
}

type DriverParams interface {
	SetDefaults()
}

// DefaultConfig returns an instance of the default configuration for Grip.
func DefaultConfig() *Config {
	c := &Config{}
	c.Server.HostName = "localhost"
	c.Server.HTTPPort = "8201"
	c.Server.RPCPort = "8202"
	c.Server.WorkDir = "grip.work"
	c.Server.ReadOnly = false
	c.Server.EnablePlugins = false
	c.Server.DisableHTTPCache = true
	c.Server.AutoBuildSchemas = false
	c.Server.SchemaRefreshInterval = duration.Duration(24 * time.Hour)
	c.Server.SchemaInspectN = 500
	c.Server.SchemaRandomSample = true
	c.Server.RequestLogging.HeaderWhitelist = []string{
		"authorization", "oauthemail", "content-type", "content-length",
		"forwarded", "x-forwarded-for", "x-forwarded-host", "user-agent",
	}

	c.RPCClient = rpc.ConfigWithDefaults(c.Server.RPCAddress())

	c.Drivers = map[string]DriverConfig{}

	c.Sources = map[string]string{}

	c.Logger = log.DefaultLoggerConfig()
	return c
}

func (conf *Config) AddBadgerDefault() {
	n := "grip.db"
	conf.Drivers["badger"] = DriverConfig{Badger: &n}
	conf.Default = "badger"
}

func (conf *Config) AddPebbleDefault() {
	n := "grip-pebble.db"
	conf.Drivers["pebble"] = DriverConfig{Pebble: &n}
	conf.Default = "pebble"
}

func (conf *Config) AddMongoDefault() {
	c := mongo.Config{}
	c.SetDefaults()
	conf.Drivers["mongo"] = DriverConfig{MongoDB: &c}
	conf.Default = "mongo"
}

// TestifyConfig randomizes ports and database paths/names
func TestifyConfig(c *Config) {
	rand := strings.ToLower(util.RandomString(6))

	c.Server.HTTPPort = util.RandomPort()
	c.Server.RPCPort = util.RandomPort()
	c.Server.WorkDir = "grip.work." + rand

	c.RPCClient.ServerAddress = c.Server.RPCAddress()

	d := c.Drivers[c.Default]

	if d.Badger != nil {
		a := "grip.db." + rand
		d.Badger = &a
	}
	if d.MongoDB != nil {
		d.MongoDB.DBName = "gripdb-" + rand
	}
	if d.Elasticsearch != nil {
		d.Elasticsearch.DBName = "gripdb-" + rand
		d.Elasticsearch.Synchronous = true
	}
	c.Drivers[c.Default] = d
}

func (c *Config) SetDefaults() {
	for _, d := range c.Drivers {
		if d.MongoDB != nil {
			d.MongoDB.SetDefaults()
		}
		if d.Elasticsearch != nil {
			d.Elasticsearch.SetDefaults()
		}
	}
}

// ParseConfig parses a YAML doc into the given Config instance.
func ParseConfig(raw []byte, conf *Config) error {
	//j, err := yaml.YAMLToJSON(raw)
	//if err != nil {
	//	return err
	//}
	//err = CheckForUnknownKeys(j, conf, []string{"Gripper.Graphs."})
	//if err != nil {
	//	return err
	//}
	err := yaml.UnmarshalStrict(raw, conf)
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
	for i := range conf.Drivers {
		if conf.Drivers[i].Gripper != nil {
			if conf.Drivers[i].Gripper.MappingFile != "" {
				gpath := filepath.Join(filepath.Dir(path), conf.Drivers[i].Gripper.MappingFile)

				gsource, err := ioutil.ReadFile(gpath)
				if err != nil {
					return fmt.Errorf("failed to read graph at path %s: \n%v", gpath, err)
				}
				// Parse file
				data := map[string]interface{}{}
				err = yaml.Unmarshal(gsource, &data)
				if err != nil {
					return fmt.Errorf("failed to parse config at path %s: \n%v", path, err)
				}
				graph, err := gripql.GraphMapToProto(data)
				if err != nil {
					return fmt.Errorf("failed to parse config at path %s: \n%v", path, err)
				}
				conf.Drivers[i].Gripper.Mapping, _ = gripper.GraphToConfig(graph)
			}
		}
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
func CheckForUnknownKeys(jsonStr []byte, obj interface{}, exclude []string) error {
	fmt.Printf("Checking: %#v\n", obj)
	if _, ok := obj.(map[string]DriverConfig); ok {
		fmt.Printf("Is map\n")
		return nil
	}
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
			for _, e := range exclude {
				if strings.HasPrefix(k, e) {
					found = true
				}
			}
			if !found {
				unknown = append(unknown, k)
			}
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
