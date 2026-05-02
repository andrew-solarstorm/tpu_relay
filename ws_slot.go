package tpu_relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/andrew-solarstorm/digo"
	"github.com/gorilla/websocket"
)

const WS_SLOT_SVC = "ws-slot-ingest-service"

type WsSlotIngestService struct {
	digo.BaseDIInstance
	scheduleSvc *ScheduleService
	conn        *websocket.Conn
	cancel      context.CancelFunc
	mu          sync.Mutex

	endpoint string
}

func (svc *WsSlotIngestService) ID() string {
	return WS_SLOT_SVC
}

func (svc *WsSlotIngestService) Configure(c digo.IContainer) error {
	svc.scheduleSvc = digo.Instance[*ScheduleService](SCHEDULER_SVC, c)
	conf := digo.GetConfig[*Config](c)
	svc.endpoint = strings.Replace(conf.RPCUrl, "http", "ws", 1)
	return nil
}

func (svc *WsSlotIngestService) Start() error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(svc.endpoint, nil)
	if err != nil {
		return fmt.Errorf("ws dial: %w", err)
	}
	svc.conn = conn

	// Subscribe to slot notifications
	sub := rpcRequest{
		Jsonrpc: "2.0",
		ID:      1,
		Method:  "slotSubscribe",
		Params:  []any{},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("slotSubscribe: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	svc.cancel = cancel

	go svc.readLoop(ctx)

	return nil
}

func (svc *WsSlotIngestService) readLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, msg, err := svc.conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return // clean shutdown
			}
			log.Printf("[slot-ingest] read error: %v — reconnecting...", err)
			svc.reconnect(ctx)
			continue
		}

		var notif slotNotification
		if err := json.Unmarshal(msg, &notif); err != nil {
			log.Printf("[slot-ingest] unmarshal error: %v", err)
			continue
		}

		// Skip subscription confirmation and other non-slot messages
		if notif.Method != "slotNotification" || notif.Params == nil {
			continue
		}

		svc.scheduleSvc.OnSlot(notif.Params.Result.Slot)
	}
}

func (svc *WsSlotIngestService) reconnect(ctx context.Context) {
	backoff := 500 * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		log.Printf("[slot-ingest] attempting reconnect to %s", svc.endpoint)

		dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}

		conn, _, err := dialer.Dial(svc.endpoint, nil)
		if err != nil {
			log.Printf("[slot-ingest] reconnect failed: %v", err)
			backoff = min(backoff*2, 10*time.Second)
			continue
		}

		sub := rpcRequest{
			Jsonrpc: "2.0",
			ID:      1,
			Method:  "slotSubscribe",
			Params:  []any{},
		}
		if err := conn.WriteJSON(sub); err != nil {
			log.Printf("[slot-ingest] resubscribe failed: %v", err)
			conn.Close()
			backoff = min(backoff*2, 10*time.Second)
			continue
		}

		svc.mu.Lock()
		svc.conn = conn
		svc.mu.Unlock()

		log.Printf("[slot-ingest] reconnected successfully")
		return
	}
}

func (svc *WsSlotIngestService) Stop() error {
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

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
