package server

import "time"

// Config describes configuration for the server.
type Config struct {
	HostName         string
	HTTPPort         string
	RPCPort          string
	WorkDir          string
	ContentDir       string
	ReadOnly         bool
	BasicAuth        []BasicCredential
	DisableHTTPCache bool
}

// HTTPAddress returns the HTTP address based on HostName and HTTPPort
func (c Config) HTTPAddress() string {
	http := ""
	if c.HostName != "" {
		http = "http://" + c.HostName
	}
	if c.HTTPPort != "" {
		http = http + ":" + c.HTTPPort
	}
	return http
}

// RPCAddress returns the RPC address based on HostName and RPCPort
func (c *Config) RPCAddress() string {
	rpc := c.HostName
	if c.RPCPort != "" {
		rpc = rpc + ":" + c.RPCPort
	}
	return rpc
}

// BasicCredential describes a username and password for use with Funnel's basic auth.
type BasicCredential struct {
	User     string
	Password string
}

// RPCClient describes configuration for gRPC clients
type RPCClient struct {
	BasicCredential
	ServerAddress string
	// The timeout to use for making RPC client connections in nanoseconds
	// This timeout is Only enforced when used in conjunction with the
	// grpc.WithBlock dial option.
	Timeout time.Duration
	// The maximum number of times that a request will be retried for failures.
	// Time between retries follows an exponential backoff starting at 5 seconds
	// up to 1 minute
	MaxRetries uint
}
