package config

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bmeg/grip/arango"
	esql "github.com/bmeg/grip/existing-sql"
	"github.com/bmeg/grip/grids"
	"github.com/bmeg/grip/gripper"
	"github.com/bmeg/grip/log"
	"github.com/bmeg/grip/mongo"
	"github.com/bmeg/grip/psql"
	"github.com/bmeg/grip/schema"
	"github.com/bmeg/grip/sqlite"
	"github.com/bmeg/grip/util"
	"github.com/bmeg/grip/util/duration"
	"github.com/bmeg/grip/util/rpc"
	"sigs.k8s.io/yaml"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

type DriverConfig struct {
	Grids       *grids.Config
	Badger      *string
	Bolt        *string
	Level       *string
	Pebble      *string
	ArangoDB    *arango.Config
	MongoDB     *mongo.Config
	PSQL        *psql.Config
	ExistingSQL *esql.Config
	Sqlite      *sqlite.Config
	Gripper     *gripper.Config
}

type KafkaConfig struct {
	Username *string
	Password *string
	Hostname *string
	// Limit to one topic stream for now
	Topic *string
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
	Kafka     KafkaConfig
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
		"content-type", "content-length", "forwarded",
		"x-forwarded-for", "x-forwarded-host", "user-agent",
	}

	c.RPCClient = rpc.ConfigWithDefaults(c.Server.RPCAddress())

	c.Drivers = map[string]DriverConfig{}

	c.Sources = map[string]string{}

	c.Logger = log.DefaultLoggerConfig()

	c.Kafka = KafkaConfig{}

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

func (conf *Config) AddArangoDefault() {
	c := arango.Config{}
	c.SetDefaults()
	conf.Drivers["arango"] = DriverConfig{ArangoDB: &c}
	conf.Default = "arango"
}

func (conf *Config) AddSqliteDefault() {
	c := sqlite.Config{DBName: "grip-sqlite.db"}
	conf.Drivers["sqlite"] = DriverConfig{Sqlite: &c}
	conf.Default = "sqlite"
}

func (conf *Config) AddGridsDefault() {
	n := "grip-grids.db"
	c := grids.Config{GraphDir: n, BulkLoaderWorkers: 10, Driver: "jsontable"}
	conf.Drivers["grids"] = DriverConfig{Grids: &c}
	conf.Default = "grids"
}

// TestifyConfig randomizes ports and database paths/names
func TestifyConfig(c *Config) {
	rand := strings.ToLower(util.RandomString(6))

	c.Server.HTTPPort = util.RandomPort()
	c.Server.RPCPort = util.RandomPort()
	c.Server.WorkDir = "grip.work." + rand

	c.RPCClient.ServerAddress = c.Server.RPCAddress()

	if c.Default == "" {
		return
	}
	d := c.Drivers[c.Default]

	if d.Badger != nil {
		a := "grip.db." + rand
		d.Badger = &a
	}
	if d.Pebble != nil {
		a := "grip.db." + rand
		d.Pebble = &a
	}
	if d.Grids != nil {
		c := *d.Grids
		c.GraphDir = "grip-grids.db." + rand
		d.Grids = &c
	}
	if d.MongoDB != nil {
		d.MongoDB.DBName = "gripdb-" + rand
	}
	if d.ArangoDB != nil {
		d.ArangoDB.DBName = "gripdb-" + rand
	}
	if d.Sqlite != nil {
		d.Sqlite.DBName = "gripdb-" + rand
	}
	c.Drivers[c.Default] = d
}

func (c *Config) SetDefaults() {
	for _, d := range c.Drivers {
		if d.ArangoDB != nil {
			d.ArangoDB.SetDefaults()
		}
		if d.MongoDB != nil {
			d.MongoDB.SetDefaults()
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
	source, err := os.ReadFile(path)
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

				gsource, err := os.ReadFile(gpath)
				if err != nil {
					return fmt.Errorf("failed to read graph at path %s: \n%v", gpath, err)
				}
				// Parse file
				data := map[string]any{}
				err = yaml.Unmarshal(gsource, &data)
				if err != nil {
					return fmt.Errorf("failed to parse config at path %s: \n%v", path, err)
				}
				graph, err := schema.GraphMapToProto(data)
				if err != nil {
					return fmt.Errorf("failed to parse config at path %s: \n%v", path, err)
				}
				conf.Drivers[i].Gripper.Mapping, _ = gripper.GraphToConfig(graph)
			}
		}
	}
	return nil
}

func DeepCopyRedactedConfig(conf *Config) *Config {
	if conf == nil {
		return nil
	}

	cpyConf := &Config{
		Server: conf.Server,
		RPCClient: rpc.Config{
			ServerAddress: conf.RPCClient.ServerAddress,
			Timeout:       conf.RPCClient.Timeout,
			MaxRetries:    conf.RPCClient.MaxRetries,
			User:          conf.RPCClient.User,
			Password:      "[REDACTED]",
		},
		Logger:  conf.Logger,
		Default: conf.Default,
		Graphs:  make(map[string]string, len(conf.Graphs)),
		Drivers: make(map[string]DriverConfig, len(conf.Drivers)),
		Sources: make(map[string]string, len(conf.Sources)),
		Kafka:   KafkaConfig{},
	}

	for k, v := range conf.Graphs {
		cpyConf.Graphs[k] = v
	}

	for k, v := range conf.Sources {
		cpyConf.Sources[k] = v
	}

	for k, driver := range conf.Drivers {
		cpyDriver := DriverConfig{}
		if driver.Grids != nil {
			grids := *driver.Grids
			cpyDriver.Grids = &grids
		}
		if driver.Badger != nil {
			badger := *driver.Badger
			cpyDriver.Badger = &badger
		}
		if driver.Bolt != nil {
			bolt := *driver.Bolt
			cpyDriver.Bolt = &bolt
		}
		if driver.Level != nil {
			level := *driver.Level
			cpyDriver.Level = &level
		}
		if driver.Pebble != nil {
			pebble := *driver.Pebble
			cpyDriver.Pebble = &pebble
		}
		if driver.ArangoDB != nil {
			arangoDB := &arango.Config{DBName: "[REDACTED]", Password: "[REDACTED]"}
			cpyDriver.ArangoDB = arangoDB
		}
		if driver.MongoDB != nil {
			mongoDB := &mongo.Config{DBName: "[REDACTED]", Password: "[REDACTED]"}
			cpyDriver.MongoDB = mongoDB
		}
		if driver.PSQL != nil {
			psql := &psql.Config{DBName: "[REDACTED]", Password: "[REDACTED]"}
			cpyDriver.PSQL = psql
		}
		if driver.ExistingSQL != nil {
			existingSQL := &esql.Config{DataSourceName: "[REDACTED]"}
			cpyDriver.ExistingSQL = existingSQL
		}
		if driver.Sqlite != nil {
			sqlite := *driver.Sqlite
			cpyDriver.Sqlite = &sqlite
		}
		if driver.Gripper != nil {
			gripper := &gripper.Config{MappingFile: "[REDACTED]"}
			cpyDriver.Gripper = gripper
		}
		cpyConf.Drivers[k] = cpyDriver
	}
	if conf.Kafka.Username != nil {
		cpyConf.Kafka.Username = conf.Kafka.Username
	}
	if conf.Kafka.Password != nil {
		password := "[REDACTED]" // Redact (tagged as sensitive)
		cpyConf.Kafka.Password = &password
	}
	if conf.Kafka.Hostname != nil {
		cpyConf.Kafka.Hostname = conf.Kafka.Hostname
	}
	if conf.Kafka.Topic != nil {
		topic := *conf.Kafka.Topic
		cpyConf.Kafka.Topic = &topic
	}
	return cpyConf
}
