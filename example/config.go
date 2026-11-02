package main

import (
	"errors"
	"os"
	"strconv"

	"github.com/andrew-solarstorm/digo"
)

type Config struct {
	RPCUrl string
	WSUrl  string

	HTTPPort int
	HTTPHost string
}

// Load implements [digo.IConfig].
func (c *Config) Load() error {
	c.RPCUrl = os.Getenv("RPC_URL")
	c.WSUrl = os.Getenv("WS_URL")

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
	if c.RPCUrl == "" {
		return errors.New("RPC_URL is required")
	}
	if c.WSUrl == "" {
		return errors.New("WS_URL is required")
	}

	return nil
}

var _ digo.IConfig = (*Config)(nil)
