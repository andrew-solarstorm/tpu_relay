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

	// max TPU connections kept open to leaders, least recently used are evicted
	MaxConn int
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

	maxConnStr := os.Getenv("MAX_CONN")
	if maxConnStr != "" {
		maxConn, err := strconv.Atoi(maxConnStr)
		if err != nil {
			return err
		}
		c.MaxConn = maxConn
	} else {
		c.MaxConn = 3000
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
	if c.MaxConn <= 0 {
		return errors.New("MAX_CONN must be positive")
	}

	return nil
}

var _ digo.IConfig = (*Config)(nil)
