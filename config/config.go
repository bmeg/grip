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

	"github.com/bmeg/arachne/elastic"
	"github.com/bmeg/arachne/mongo"
	"github.com/bmeg/arachne/util"
	"github.com/ghodss/yaml"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

// Config describes the configuration for Arachne.
type Config struct {
	Database string
	Server   struct {
		HTTPPort   string
		RPCPort    string
		WorkDir    string
		ContentDir string
		ReadOnly   bool
	}
	KVStorePath   string
	ElasticSearch elastic.Config
	MongoDB       mongo.Config
}

// DefaultConfig returns an instance of the default configuration for Arachne.
func DefaultConfig() *Config {
	c := &Config{}
	c.Database = "badger"
	c.Server.HTTPPort = "8201"
	c.Server.RPCPort = "8202"
	c.Server.WorkDir = "arachne.work"
	c.Server.ReadOnly = false
	c.KVStorePath = "arachne.db"
	c.MongoDB.DBName = "arachnedb"
	c.MongoDB.BatchSize = 1000
	c.MongoDB.UseAggregationPipeline = true
	c.ElasticSearch.DBName = "arachnedb"
	c.ElasticSearch.BatchSize = 1000
	return c
}

// randomPort returns a random port string between 10000 and 20000.
func randomPort() string {
	min := 10000
	max := 40000
	n := rand.Intn(max-min) + min
	return fmt.Sprintf("%d", n)
}

// TestifyConfig randomizes ports and database paths/names
func TestifyConfig(c *Config) {
	rand := strings.ToLower(util.RandomString(6))
	c.Server.HTTPPort = randomPort()
	c.Server.RPCPort = randomPort()
	c.Server.WorkDir = "arachne.work." + rand
	c.KVStorePath = "arachne.db." + rand
	c.MongoDB.DBName = "arachnedb-" + rand
	c.ElasticSearch.DBName = "arachnedb-" + rand
	c.ElasticSearch.Synchronous = true
}

// ParseConfig parses a YAML doc into the given Config instance.
func ParseConfig(raw []byte, conf *Config) error {
	j, err := yaml.YAMLToJSON(raw)
	if err != nil {
		return err
	}
	err = checkForUnknownKeys(j, conf)
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

func getKeys(obj interface{}) []string {
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

			valKeys := getKeys(field.Interface())
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

			valKeys := getKeys(v.MapIndex(key).Interface())
			for i, v := range valKeys {
				valKeys[i] = name + "." + v
			}
			keys = append(keys, valKeys...)
		}
	}

	return keys
}

func checkForUnknownKeys(jsonStr []byte, obj interface{}) error {
	knownMap := make(map[string]interface{})
	known := getKeys(obj)
	for _, k := range known {
		knownMap[k] = nil
	}

	var anon interface{}
	err := json.Unmarshal(jsonStr, &anon)
	if err != nil {
		return err
	}

	unknown := []string{}
	all := getKeys(anon)
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
