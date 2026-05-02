package tpu_relay

import (
	"github.com/gagliardetto/solana-go"
	"github.com/quic-go/quic-go"
)

type LeaderConn struct {
	QUICURL string
	Conn    *quic.Conn
}

type Leader struct {
	Pubkey  solana.PublicKey `json:"pubkey"`
	TPUQUIC string           `json:"tpuquic"`
	QUIC    string
}

type LeaderAtSlot struct {
	Slot uint64
	Info *Leader
}

// ws request
type rpcRequest struct {
	Jsonrpc string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type slotNotification struct {
	Jsonrpc string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  *struct {
		Result struct {
			Slot   uint64 `json:"slot"`
			Parent uint64 `json:"parent"`
			Root   uint64 `json:"root"`
		} `json:"result"`
		Subscription int64 `json:"subscription"`
	} `json:"params"`
}
