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

type EpochSchedule struct {
	Start, End uint64
}
