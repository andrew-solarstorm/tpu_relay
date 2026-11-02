package tpu_relay

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/quic-go/quic-go"
	"github.com/rs/zerolog/log"
)

type tpuDiscover struct {
	// poll LRU is already thread-safe
	pool *lru.Cache[solana.PublicKey, *LeaderConn]
	cert *tls.Config

	mut        sync.RWMutex
	connecting map[solana.PublicKey]struct{}
}

func NewTPUDiscover() (*tpuDiscover, error) {
	svc := tpuDiscover{}
	cache, err := lru.NewWithEvict(100, svc.onEvict)
	if err != nil {
		return nil, err
	}
	svc.pool = cache
	cert, err := createTlsConfig()
	if err != nil {
		return nil, err
	}
	svc.cert = cert
	svc.connecting = make(map[solana.PublicKey]struct{})
	return &svc, nil
}

func (svc *tpuDiscover) onEvict(pk solana.PublicKey, leader *LeaderConn) {
	log.Info().Str("PublicKey", pk.String()).Msg("Evict msg")
	_ = leader.Conn.CloseWithError(quic.ApplicationErrorCode(quic.NoError), "client close")
}

func (svc *tpuDiscover) IsConnecting(pk solana.PublicKey) bool {
	svc.mut.RLock()
	defer svc.mut.RUnlock()
	_, ok := svc.connecting[pk]
	return ok
}

func (svc *tpuDiscover) SetConnecting(pk solana.PublicKey, connecting bool) {
	svc.mut.Lock()
	defer svc.mut.Unlock()
	if connecting {
		svc.connecting[pk] = struct{}{}
	} else {
		delete(svc.connecting, pk)
	}
}

func (svc *tpuDiscover) IsConnected(pk solana.PublicKey) bool {
	_, ok := svc.pool.Peek(pk)
	return ok
}

func (svc *tpuDiscover) Connect(ctx context.Context, leader *LeaderAtSlot) (*LeaderConn, error) {
	if conn, ok := svc.pool.Peek(leader.Info.Pubkey); ok {
		if conn.Conn.Context().Err() == nil {
			return conn, nil
		}
		svc.pool.Remove(leader.Info.Pubkey)
	}

	return svc.connect(ctx, leader)
}

func (svc *tpuDiscover) connect(ctx context.Context, leader *LeaderAtSlot) (*LeaderConn, error) {
	svc.SetConnecting(leader.Info.Pubkey, true)
	defer svc.SetConnecting(leader.Info.Pubkey, false)

	tn := time.Now()
	udpAddr, err := net.ResolveUDPAddr("udp", leader.Info.TPUQUIC)
	if err != nil {
		log.Error().
			Err(err).
			Str("quic", leader.Info.TPUQUIC).
			Str("leader", leader.Info.Pubkey.String()).
			Msg("TPUService::connect error")
		return nil, err
	}

	conn, err := quic.DialAddr(ctx, udpAddr.String(), svc.cert, &quic.Config{
		MaxIdleTimeout:       30 * time.Second,
		HandshakeIdleTimeout: 10 * time.Second,
		KeepAlivePeriod:      15 * time.Second,
	})
	if err != nil {
		log.Error().
			Err(err).
			Any("leader", leader).
			Str("leader", leader.Info.Pubkey.String()).
			Msg("TPUService::connect dial error")
		return nil, err
	}

	log.Info().
		Str("quic", leader.Info.TPUQUIC).
		Str("leader", leader.Info.Pubkey.String()).
		Msgf("TPUService::connect Dial took: %s", time.Since(tn))
	leaderConn := &LeaderConn{
		QUICURL: leader.Info.TPUQUIC,
		Conn:    conn,
	}

	svc.pool.Add(leader.Info.Pubkey, leaderConn)
	return leaderConn, nil
}

func (svc *tpuDiscover) Send(ctx context.Context, leader *LeaderAtSlot, rawTx []byte) error {
	conn, err := svc.Connect(ctx, leader)
	if err != nil {
		return err
	}

	stream, err := conn.Conn.OpenUniStreamSync(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()

	_, err = stream.Write(rawTx)
	if err != nil {
		return err
	}
	return nil
}
