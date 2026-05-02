package tpu_relay

import (
	"os"
	"strconv"

	"github.com/andrew-solarstorm/digo"
)

type Config struct {
	RPCUrl    string
	GRPCUrl   string
	GRPCToken string

	HTTPPort int
	HTTPHost string
}

// Load implements [digo.IConfig].
func (c *Config) Load() error {
	c.RPCUrl = os.Getenv("RPC_URL")
	c.GRPCUrl = os.Getenv("YELLOWSTONE_URL")
	c.GRPCToken = os.Getenv("YELLOWSTONE_TOKEN")

	c.HTTPHost = os.Getenv("HTTP_HOST")
	if c.HTTPHost == "" {
		c.HTTPHost = "localhost"
	}
	portStr := os.Getenv("HTTP_PORT")
	if portStr != "" {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return err
		}
		c.HTTPPort = port
	} else {
		c.HTTPPort = 8080
	}
	return nil
}

// Validate implements [digo.IConfig].
func (c *Config) Validate() error {
	if c.GRPCUrl == "" || c.GRPCToken == "" || c.RPCUrl == "" {
		return nil
	}

	return nil
}

var _ digo.IConfig = (*Config)(nil)
