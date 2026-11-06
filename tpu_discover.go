package tpu_relay

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/quic-go/quic-go"
	"github.com/rs/zerolog/log"
)

type dialCall struct {
	done chan struct{}
	conn *LeaderConn
	err  error
}

// tpuDiscover pools one QUIC connection per TPU endpoint (host:port), not per
// validator. Many validators share an endpoint (e.g. a BAM node), and a TPU
// server limits connections per source IP for unstaked clients like this one,
// so a second connection to the same endpoint is closed with "too_many".
type tpuDiscover struct {
	ctx context.Context

	pool *lru.Cache[string, *LeaderConn]
	cert *tls.Config

	mut     sync.Mutex
	dialing map[string]*dialCall
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
	svc.dialing = make(map[string]*dialCall)
	return &svc, nil
}

func (svc *tpuDiscover) onEvict(addr string, leader *LeaderConn) {
	log.Debug().Str("quic", addr).Msg("Evict msg")
	_ = leader.Conn.CloseWithError(quic.ApplicationErrorCode(quic.NoError), "client close")
}

func (svc *tpuDiscover) Close() {
	svc.pool.Purge()
}

func (svc *tpuDiscover) live(addr string) *LeaderConn {
	if conn, ok := svc.pool.Get(addr); ok && conn.Conn.Context().Err() == nil {
		return conn
	}
	return nil
}

func (svc *tpuDiscover) Connect(ctx context.Context, leader *LeaderAtSlot) (*LeaderConn, error) {
	addr := leader.Leader.TPUQUIC
	if conn := svc.live(addr); conn != nil {
		return conn, nil
	}

	svc.mut.Lock()
	if conn := svc.live(addr); conn != nil {
		svc.mut.Unlock()
		return conn, nil
	}
	call, inflight := svc.dialing[addr]
	if !inflight {
		svc.pool.Remove(addr)
		call = &dialCall{done: make(chan struct{})}
		svc.dialing[addr] = call
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
		svc.pool.Add(leader.Leader.TPUQUIC, call.conn)
	}
	delete(svc.dialing, leader.Leader.TPUQUIC)
	svc.mut.Unlock()

	close(call.done)
}

func (svc *tpuDiscover) connect(ctx context.Context, leader *LeaderAtSlot) (*LeaderConn, error) {
	tn := time.Now()
	udpAddr, err := net.ResolveUDPAddr("udp", leader.Leader.TPUQUIC)
	if err != nil {
		log.Warn().
			Err(err).
			Str("quic", leader.Leader.TPUQUIC).
			Str("leader", leader.Leader.Pubkey.String()).
			Msg("TPUService::connect error")
		return nil, err
	}

	conn, err := quic.DialAddr(ctx, udpAddr.String(), svc.cert, &quic.Config{
		MaxIdleTimeout:       30 * time.Second,
		HandshakeIdleTimeout: 10 * time.Second,
		KeepAlivePeriod:      15 * time.Second,
	})
	if err != nil {
		log.Warn().
			Err(err).
			Str("leader", leader.Leader.Pubkey.String()).
			Msg("TPUService::connect dial error")
		return nil, err
	}

	log.Debug().
		Str("quic", leader.Leader.TPUQUIC).
		Str("leader", leader.Leader.Pubkey.String()).
		Str("name", leader.Leader.Name).
		Str("country", leader.Leader.IPCountry).
		Str("city", leader.Leader.IPCity).
		Bool("is JITO", leader.Leader.IsJITO).
		Str("client", leader.Leader.Client).
		Uint64("slot", leader.Slot).
		Msgf("TPUService::connect Dial took: %s", time.Since(tn))

	return &LeaderConn{
		QUICURL: leader.Leader.TPUQUIC,
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
