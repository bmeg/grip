package grids

import "runtime"

type Config struct {
	GraphDir          string
	BulkLoaderWorkers int
	Driver            string
}

func (c *Config) SetDefaults() {
	if c.BulkLoaderWorkers == 0 {
		c.BulkLoaderWorkers = runtime.NumCPU()
	}
}
