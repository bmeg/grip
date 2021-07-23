package gripper

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type tableState int

const (
	tableNull tableState = iota
	tableLoading
	tableLoaded
)

type DriverCache struct {
	Driver Driver

	tableLock  *sync.RWMutex
	tableState tableState
	tableCache map[string]*BaseRow
	tableKeys  []string
	tableTime  int64
}

func NewDriverCache(dr Driver) *DriverCache {
	return &DriverCache{Driver: dr, tableState: tableNull, tableLock: &sync.RWMutex{}}
}

func (dc *DriverCache) GetTimeout() int {
	return dc.Driver.GetTimeout()
}

func (dc *DriverCache) GetFields() ([]string, error) {
	return dc.Driver.GetFields()
}

func (dc *DriverCache) FetchRow(id string) (*BaseRow, error) {
	dc.tableLock.RLock()
	if dc.tableState == tableLoaded {
		defer dc.tableLock.RUnlock()
		if out, ok := dc.tableCache[id]; ok {
			return out, nil
		}
		return nil, fmt.Errorf("Not Found")
	}
	dc.tableLock.RUnlock()
	return dc.Driver.FetchRow(id)
}

func (dc *DriverCache) reloadRequired() bool {
	dc.tableLock.RLock()
	defer dc.tableLock.RUnlock()
	return dc.tableState != tableLoaded || dc.tableTime+int64(dc.GetTimeout()) < time.Now().Unix()
}

func (dc *DriverCache) startReload() {
	dc.tableLock.Lock()
	if dc.tableState == tableLoading {
		dc.tableLock.Unlock()
		return
	}
	dc.tableState = tableLoading
	dc.tableKeys = []string{}
	dc.tableCache = map[string]*BaseRow{}
	dc.tableLock.Unlock()
	go func() {
		rg, _ := dc.Driver.FetchRows(context.Background())
		for row := range rg {
			dc.tableLock.Lock()
			dc.tableKeys = append(dc.tableKeys, row.Key)
			dc.tableCache[row.Key] = row
			dc.tableLock.Unlock()
		}
		dc.tableLock.Lock()
		dc.tableState = tableLoaded
		dc.tableTime = time.Now().Unix()
		dc.tableLock.Unlock()
	}()
}

func (dc *DriverCache) FetchRows(ctx context.Context) (chan *BaseRow, error) {
	if dc.reloadRequired() {
		dc.startReload()
	}
	out := make(chan *BaseRow, 10)
	go func() {
		defer close(out)
		count := 0
		for stillReading := true; stillReading; {
			dc.tableLock.RLock()
			for i := count; i < len(dc.tableKeys); i++ {
				k := dc.tableKeys[i]
				v := dc.tableCache[k]
				out <- v
			}
			if dc.tableState == tableLoaded || ctx.Err() == context.Canceled {
				stillReading = false
			} else {
				count = len(dc.tableKeys)
				time.Sleep(100 * time.Millisecond)
			}
			dc.tableLock.RUnlock()
		}
	}()
	return out, nil
}

func (dc *DriverCache) FetchMatchRows(ctx context.Context, field string, value string) (chan *BaseRow, error) {

	out := make(chan *BaseRow, 10)
	go func() {
		defer close(out)
		dc.tableLock.RLock()
		if dc.tableState != tableLoaded {
			dc.tableLock.RUnlock()
			rc, _ := dc.Driver.FetchMatchRows(ctx, field, value)
			for row := range rc {
				out <- row
			}
		} else {
			for i := 0; i < len(dc.tableKeys); i++ {
				k := dc.tableKeys[i]
				v := dc.tableCache[k]
				if f, ok := v.Value[field]; ok {
					if fStr, ok := f.(string); ok {
						if fStr == value {
							out <- v
						}
					}
				}
			}
			dc.tableLock.RUnlock()
		}
	}()

	return out, nil
}

type DriverPreLoad struct {
	tableCache map[string]*BaseRow
	tableKeys  []string
}

func NewDriverPreload(data map[string]*BaseRow) *DriverPreLoad {
	keys := []string{}
	for k := range data {
		keys = append(keys, k)
	}
	return &DriverPreLoad{data, keys}
}

func (dp *DriverPreLoad) GetTimeout() int {
	return -1
}

func (dp *DriverPreLoad) GetFields() ([]string, error) {
	fields := map[string]bool{}

	for _, row := range dp.tableCache {
		for k, v := range row.Value {
			if _, ok := v.(string); ok {
				fields[k] = true
			}
		}
	}

	out := []string{}
	for k := range fields {
		out = append(out, k)
	}
	return out, nil
}

func (dp *DriverPreLoad) FetchRow(id string) (*BaseRow, error) {
	if out, ok := dp.tableCache[id]; ok {
		return out, nil
	}
	return nil, fmt.Errorf("Not Found")
}

func (dp *DriverPreLoad) FetchRows(ctx context.Context) (chan *BaseRow, error) {
	out := make(chan *BaseRow, 10)
	go func() {
		defer close(out)
		count := 0
		for i := count; i < len(dp.tableKeys); i++ {
			k := dp.tableKeys[i]
			v := dp.tableCache[k]
			out <- v
		}
	}()
	return out, nil
}

func (dp *DriverPreLoad) FetchMatchRows(ctx context.Context, field string, value string) (chan *BaseRow, error) {
	out := make(chan *BaseRow, 10)
	go func() {
		defer close(out)
		for i := 0; i < len(dp.tableKeys); i++ {
			k := dp.tableKeys[i]
			v := dp.tableCache[k]
			if f, ok := v.Value[field]; ok {
				if fStr, ok := f.(string); ok {
					if fStr == value {
						out <- v
					}
				}
			}
		}
	}()
	return out, nil
}
