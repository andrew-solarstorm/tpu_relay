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

type dialCall struct {
	done chan struct{}
	conn *LeaderConn
	err  error
}

type tpuDiscover struct {
	ctx context.Context

	pool *lru.Cache[solana.PublicKey, *LeaderConn]
	cert *tls.Config

	mut     sync.Mutex
	dialing map[solana.PublicKey]*dialCall
}

func NewTPUDiscover(ctx context.Context, maxCon int) (*tpuDiscover, error) {
	svc := tpuDiscover{ctx: ctx}
	cache, err := lru.NewWithEvict(maxCon, svc.onEvict)
	if err != nil {
		return nil, err
	}
	svc.pool = cache
	cert, err := createTlsConfig()
	if err != nil {
		return nil, err
	}
	svc.cert = cert
	svc.dialing = make(map[solana.PublicKey]*dialCall)
	return &svc, nil
}

func (svc *tpuDiscover) onEvict(pk solana.PublicKey, leader *LeaderConn) {
	log.Info().Str("PublicKey", pk.String()).Msg("Evict msg")
	_ = leader.Conn.CloseWithError(quic.ApplicationErrorCode(quic.NoError), "client close")
}

func (svc *tpuDiscover) Close() {
	svc.pool.Purge()
}

func (svc *tpuDiscover) live(pk solana.PublicKey) *LeaderConn {
	if conn, ok := svc.pool.Get(pk); ok && conn.Conn.Context().Err() == nil {
		return conn
	}
	return nil
}

func (svc *tpuDiscover) Connect(ctx context.Context, leader *LeaderAtSlot) (*LeaderConn, error) {
	pk := leader.Info.Pubkey
	if conn := svc.live(pk); conn != nil {
		return conn, nil
	}

	svc.mut.Lock()
	if conn := svc.live(pk); conn != nil {
		svc.mut.Unlock()
		return conn, nil
	}
	call, inflight := svc.dialing[pk]
	if !inflight {
		svc.pool.Remove(pk)
		call = &dialCall{done: make(chan struct{})}
		svc.dialing[pk] = call
		go svc.dial(call, leader)
	}
	svc.mut.Unlock()

	select {
	case <-call.done:
		return call.conn, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (svc *tpuDiscover) dial(call *dialCall, leader *LeaderAtSlot) {
	call.conn, call.err = svc.connect(svc.ctx, leader)

	svc.mut.Lock()
	if call.err == nil {
		svc.pool.Add(leader.Info.Pubkey, call.conn)
	}
	delete(svc.dialing, leader.Info.Pubkey)
	svc.mut.Unlock()

	close(call.done)
}

func (svc *tpuDiscover) connect(ctx context.Context, leader *LeaderAtSlot) (*LeaderConn, error) {
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
			Str("leader", leader.Info.Pubkey.String()).
			Msg("TPUService::connect dial error")
		return nil, err
	}

	log.Info().
		Str("quic", leader.Info.TPUQUIC).
		Str("leader", leader.Info.Pubkey.String()).
		Uint64("slot", leader.Slot).
		Msgf("TPUService::connect Dial took: %s", time.Since(tn))

	return &LeaderConn{
		QUICURL: leader.Info.TPUQUIC,
		Conn:    conn,
	}, nil
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

	if _, err = stream.Write(rawTx); err != nil {
		stream.CancelWrite(0)
		return err
	}
	return stream.Close()
}
