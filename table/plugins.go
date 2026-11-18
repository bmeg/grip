package table

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"plugin"
	"runtime"
	"sort"
	"strings"
)

const (
	// ConstructorSymbol is the exported constructor required by dynamic table plugins.
	ConstructorSymbol = "NewTableDriver"
	// ABIName identifies the supported top-level table plugin ABI.
	ABIName = "grip-table"
	// ABIVersion is incremented when the exported constructor contract changes.
	ABIVersion = 1
)

var (
	ErrPluginNotFound = errors.New("table plugin not found")
	ErrPluginABI      = errors.New("incompatible table plugin ABI")
)

// PluginInfo describes a discoverable dynamic table plugin.
type PluginInfo struct {
	Name       string
	Path       string
	SHA256     string
	Size       int64
	ABIName    string
	ABIVersion int
}

// DiscoverPlugins finds dynamic Go plugins in dir. Files must use the
// .so suffix on Linux and macOS.
func DiscoverPlugins(dir string) ([]PluginInfo, error) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		return nil, fmt.Errorf("%w: dynamic plugins are unsupported on %s", ErrPluginABI, runtime.GOOS)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPluginNotFound, err)
	}
	out := []PluginInfo{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".so") {
			continue
		}
		path := filepath.Join(dir, name)
		info, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("reading table plugin %q: %w", path, err)
		}
		checksum, err := fileSHA256(path)
		if err != nil {
			return nil, fmt.Errorf("checksumming table plugin %q: %w", path, err)
		}
		out = append(out, PluginInfo{
			Name:       strings.TrimSuffix(name, ".so"),
			Path:       path,
			SHA256:     checksum,
			Size:       info.Size(),
			ABIName:    ABIName,
			ABIVersion: ABIVersion,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

// LoadedPlugin holds a plugin module and the table driver returned by it.
// Dynamic Go plugins cannot be unloaded while the process owns references,
// so users must close the driver and then replace the process for hot reload.
type LoadedPlugin struct {
	Info   PluginInfo
	Driver TableDriver
}

// LoadPlugin opens a dynamic Go plugin and calls its exported constructor.
// config is passed unchanged to the plugin; row data never crosses RPC.
func LoadPlugin(path string, config map[string]interface{}) (*LoadedPlugin, error) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		return nil, fmt.Errorf("%w: dynamic plugins are unsupported on %s", ErrPluginABI, runtime.GOOS)
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrPluginNotFound, path)
		}
		return nil, fmt.Errorf("opening table plugin %q: %w", path, err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%w: %s is a directory", ErrPluginNotFound, path)
	}
	checksum, err := fileSHA256(path)
	if err != nil {
		return nil, fmt.Errorf("checksumming table plugin %q: %w", path, err)
	}

	module, err := plugin.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: opening %q: %v", ErrPluginABI, path, err)
	}
	symbol, err := module.Lookup(ConstructorSymbol)
	if err != nil {
		return nil, fmt.Errorf("%w: plugin %q has no %s constructor: %v",
			ErrPluginABI, path, ConstructorSymbol, err)
	}
	constructor, ok := symbol.(func(map[string]interface{}) (TableDriver, error))
	if !ok {
		return nil, fmt.Errorf("%w: plugin %q constructor has the wrong signature",
			ErrPluginABI, path)
	}
	driver, err := constructor(config)
	if err != nil {
		return nil, fmt.Errorf("constructing table driver from %q: %w", path, err)
	}
	if driver == nil {
		return nil, fmt.Errorf("%w: plugin %q returned a nil table driver", ErrPluginABI, path)
	}
	if !driver.Capabilities().Readable && !driver.Capabilities().Writable {
		return nil, fmt.Errorf("%w: plugin %q declares no readable or writable capability",
			ErrPluginABI, path)
	}

	name := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	return &LoadedPlugin{
		Info: PluginInfo{
			Name: name, Path: path, SHA256: checksum, Size: info.Size(),
			ABIName: ABIName, ABIVersion: ABIVersion,
		},
		Driver: driver,
	}, nil
}

// Close closes the table driver if it implements CloseTableDriver.
func (p *LoadedPlugin) Close() error {
	if p == nil || p.Driver == nil {
		return nil
	}
	if closer, ok := p.Driver.(CloseTableDriver); ok {
		return closer.Close()
	}
	return nil
}
