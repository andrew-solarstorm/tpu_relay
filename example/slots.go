package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/andrew-solarstorm/digo"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

const SLOT_FEED_SVC = "slot-feed-service"

type slotNotification struct {
	Method string `json:"method"`
	Params *struct {
		Result struct {
			Slot uint64 `json:"slot"`
		} `json:"result"`
	} `json:"params"`
}

// SlotFeedService subscribes to slots over the RPC websocket and pushes them
// into the relay, which uses them to pick and pre-connect to upcoming leaders.
type SlotFeedService struct {
	digo.BaseDIInstance

	relaySvc *RelayService
	endpoint string

	cancel context.CancelFunc

	mu   sync.Mutex
	conn *websocket.Conn
}

func (svc *SlotFeedService) ID() string {
	return SLOT_FEED_SVC
}

func (svc *SlotFeedService) Configure(c digo.IContainer) error {
	conf := digo.GetConfig[*Config](c)
	svc.endpoint = conf.WSUrl
	svc.relaySvc = digo.Instance[*RelayService](RELAY_SVC, c)
	return nil
}

func (svc *SlotFeedService) Start() error {
	conn, err := svc.dial()
	if err != nil {
		return err
	}
	svc.setConn(conn)

	ctx, cancel := context.WithCancel(context.Background())
	svc.cancel = cancel

	go svc.readLoop(ctx, conn)
	return nil
}

func (svc *SlotFeedService) Stop() error {
	if svc.cancel != nil {
		svc.cancel()
	}

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if svc.conn != nil {
		return svc.conn.Close()
	}
	return nil
}

func (svc *SlotFeedService) setConn(conn *websocket.Conn) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	svc.conn = conn
}

func (svc *SlotFeedService) dial() (*websocket.Conn, error) {
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.Dial(svc.endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("ws dial: %w", err)
	}

	sub := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "slotSubscribe",
		"params":  []any{},
	}
	if err := conn.WriteJSON(sub); err != nil {
		conn.Close()
		return nil, fmt.Errorf("slotSubscribe: %w", err)
	}
	return conn, nil
}

func (svc *SlotFeedService) readLoop(ctx context.Context, conn *websocket.Conn) {
	relay := svc.relaySvc.Relay()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return // clean shutdown
			}
			log.Warn().Err(err).Msg("slot feed disconnected, reconnecting")
			if conn = svc.reconnect(ctx); conn == nil {
				return
			}
			continue
		}

		var notif slotNotification
		if err := json.Unmarshal(msg, &notif); err != nil {
			log.Error().Err(err).Msg("failed to decode slot notification")
			continue
		}

		// skip the subscription confirmation and anything that isn't a slot
		if notif.Method != "slotNotification" || notif.Params == nil {
			continue
		}

		relay.OnSlot(notif.Params.Result.Slot)
	}
}

// reconnect retries with backoff until it succeeds, or returns nil once ctx is done.
func (svc *SlotFeedService) reconnect(ctx context.Context) *websocket.Conn {
	backoff := 500 * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}

		conn, err := svc.dial()
		if err != nil {
			log.Warn().Err(err).Msg("slot feed reconnect failed")
			backoff = min(backoff*2, 10*time.Second)
			continue
		}

		svc.setConn(conn)
		log.Info().Msg("slot feed reconnected")
		return conn
	}
}
