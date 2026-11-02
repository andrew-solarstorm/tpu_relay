package main

import (
	"context"

	"github.com/andrew-solarstorm/digo"
	"github.com/andrew-solarstorm/tpu_relay"
)

const RELAY_SVC = "relay-service"

// RelayService owns the tpu_relay.Relay so the other services can share it.
type RelayService struct {
	digo.BaseDIInstance

	rpcUrl  string
	maxConn int

	relay  *tpu_relay.Relay
	cancel context.CancelFunc
}

func (svc *RelayService) ID() string {
	return RELAY_SVC
}

func (svc *RelayService) Configure(c digo.IContainer) error {
	conf := digo.GetConfig[*Config](c)
	svc.rpcUrl = conf.RPCUrl
	svc.maxConn = conf.MaxConn
	return nil
}

func (svc *RelayService) Start() error {
	ctx, cancel := context.WithCancel(context.Background())

	// loads the cluster nodes and leader schedule from the RPC.
	relay, err := tpu_relay.NewRelay(ctx, svc.rpcUrl, svc.maxConn)
	if err != nil {
		cancel()
		return err
	}

	svc.relay = relay
	svc.cancel = cancel
	return nil
}

func (svc *RelayService) Stop() error {
	if svc.cancel != nil {
		svc.cancel()
	}
	return nil
}

// Relay is only valid once the service has started.
func (svc *RelayService) Relay() *tpu_relay.Relay {
	return svc.relay
}
