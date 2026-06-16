package grids

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bmeg/grip/grids/driver"
	"github.com/bmeg/grip/gripql"
	"github.com/bmeg/grip/timestamp"
)

// Graph implements the GDB interface using a genertic key/value storage driver
type Graph struct {
	graphID string

	driver            *driver.GridKVDriver
	ts                *timestamp.Timestamp
	tempDeletedEdges  map[string]struct{}
	edgesMutex        sync.Mutex
	BulkLoaderWorkers int
}

// Close the connection
func (g *Graph) Close() error {
	g.driver.Close()
	return nil
}

// AddGraph creates a new graph named `graph`
func (kgraph *GDB) AddGraph(graph string) error {
	err := gripql.ValidateGraphName(graph)
	if err != nil {
		return err
	}
	g, err := newGraph(kgraph.conf, graph)
	if err != nil {
		return err
	}
	kgraph.mu.Lock()
	defer kgraph.mu.Unlock()
	kgraph.drivers[graph] = g
	return nil
}

func newGraph(conf Config, name string) (*Graph, error) {
	dbPath := filepath.Join(conf.GraphDir, name)
	fmt.Printf("Creating new GRIDS graph %s\n", name)

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if err := os.Mkdir(dbPath, 0700); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %v", dbPath, err)
		}
	}

	versionPath := filepath.Join(dbPath, "VERSION")
	if err := os.WriteFile(versionPath, []byte("0.0.1"), 0644); err != nil {
		return nil, fmt.Errorf("failed to create VERSION file: %v", err)
	}

	drvr, err := openGridKVDriverWithRetry(conf, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open grids storage at %s: %v", dbPath, err)
	}

	ts := timestamp.NewTimestamp()

	o := &Graph{
		driver:            drvr,
		ts:                &ts,
		graphID:           name,
		tempDeletedEdges:  make(map[string]struct{}),
		edgesMutex:        sync.Mutex{},
		BulkLoaderWorkers: conf.BulkLoaderWorkers,
	}
	return o, nil
}

func getGraph(conf Config, name string) (*Graph, error) {
	dbPath := filepath.Join(conf.GraphDir, name)
	fmt.Printf("fetching GRIDS graph %s\n", name)

	versionPath := filepath.Join(dbPath, "VERSION")
	file, err := os.Open(versionPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open VERSION file at %s: %v", versionPath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		version := scanner.Text()
		if strings.TrimSpace(version) != "0.0.1" {
			return nil, fmt.Errorf("unsupported version %s", version)
		}
	}

	drvr, err := openGridKVDriverWithRetry(conf, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open grids storage at %s: %v", dbPath, err)
	}

	ts := timestamp.NewTimestamp()

	o := &Graph{
		driver:            drvr,
		ts:                &ts,
		graphID:           name,
		tempDeletedEdges:  make(map[string]struct{}),
		edgesMutex:        sync.Mutex{},
		BulkLoaderWorkers: conf.BulkLoaderWorkers,
	}
	return o, nil
}

/*
Since each graph has its own directory, delete the directory to delete the graph
*/
func (kgraph *GDB) DeleteGraph(graph string) error {
	err := gripql.ValidateGraphName(graph)
	if err != nil {
		return nil
	}
	kgraph.mu.Lock()
	defer kgraph.mu.Unlock()
	if d, ok := kgraph.drivers[graph]; ok {
		d.Close()
		delete(kgraph.drivers, graph)
	}
	dbPath := filepath.Join(kgraph.conf.GraphDir, graph)
	os.RemoveAll(dbPath)
	return nil
}

func openGridKVDriverWithRetry(conf Config, dbPath string) (*driver.GridKVDriver, error) {
	lockWaitSeconds := getenvInt("GRIDS_OPEN_LOCK_WAIT_SECONDS", 120)
	retryMillis := getenvInt("GRIDS_OPEN_LOCK_RETRY_MILLIS", 1000)
	if retryMillis <= 0 {
		retryMillis = 1000
	}

	deadline := time.Now().Add(time.Duration(lockWaitSeconds) * time.Second)
	attempt := 0
	for {
		drvr, err := driver.NewGridKVDriver(dbPath, conf.Driver)
		if err == nil {
			if attempt > 0 {
				fmt.Printf("GRIDS lock resolved path=%s attempts=%d\n", dbPath, attempt+1)
			}
			return drvr, nil
		}
		if !isLikelyFileLockError(err) || lockWaitSeconds <= 0 || time.Now().After(deadline) {
			return nil, err
		}

		attempt++
		if attempt == 1 || attempt%10 == 0 {
			remaining := time.Until(deadline).Round(time.Second)
			fmt.Printf("GRIDS lock wait path=%s attempt=%d remaining=%s err=%v\n", dbPath, attempt, remaining, err)
		}
		time.Sleep(time.Duration(retryMillis) * time.Millisecond)
	}
}

func isLikelyFileLockError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	// Some backends bubble lock contention as plain EAGAIN text without the
	// word "lock", e.g. "resource temporarily unavailable".
	if strings.Contains(s, "resource temporarily unavailable") ||
		strings.Contains(s, "database is locked") ||
		strings.Contains(s, "eagain") {
		return true
	}
	if !strings.Contains(s, "lock") {
		return false
	}
	return strings.Contains(s, "resource temporarily unavailable") ||
		strings.Contains(s, "held by") ||
		strings.Contains(s, "another process") ||
		strings.Contains(s, "is locked") ||
		strings.Contains(s, "cannot acquire") ||
		strings.Contains(s, "timeout")
}

func getenvInt(key string, def int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return v
}
