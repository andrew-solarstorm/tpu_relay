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
	Pubkey     solana.PublicKey
	VotePubkey solana.PK
	TPUQUIC    string
	Name       string
	IPCountry  string
	IPCity     string
	Client     string
	IsJITO     bool
}

type LeaderAtSlot struct {
	Slot   uint64
	Leader *Leader
}

type EpochSchedule struct {
	Start, End uint64
}
