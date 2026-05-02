package tpu_relay

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/andrew-solarstorm/digo"
	"github.com/gagliardetto/solana-go"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/quic-go/quic-go"
	"github.com/rs/zerolog/log"
)

const TPU_SVC = "tpu-service"

type TPUService struct {
	digo.BaseDIInstance

	// poll LRU is already thread-safe
	pool *lru.Cache[solana.PublicKey, *LeaderConn]
	cert *tls.Config

	mut        sync.RWMutex
	connecting map[solana.PublicKey]struct{}

	scheduler *ScheduleService
}

func (svc *TPUService) ID() string {
	return TPU_SVC
}

func (svc *TPUService) Configure(c digo.IContainer) error {
	cache, err := lru.NewWithEvict[solana.PublicKey, *LeaderConn](100, svc.onEvict)
	if err != nil {
		return err
	}
	svc.pool = cache
	cert, err := createTlsConfig()
	if err != nil {
		return err
	}
	svc.cert = cert
	svc.connecting = make(map[solana.PublicKey]struct{})

	svc.scheduler = digo.Instance[*ScheduleService](SCHEDULER_SVC, c)
	return nil
}

func (svc *TPUService) Start() error {
	return nil
}

func (svc *TPUService) Stop() error {
	return nil
}

func (svc *TPUService) onEvict(pk solana.PublicKey, leader *LeaderConn) {
	log.Info().Str("PublicKey", pk.String()).Msg("Evict msg")
	leader.Conn.CloseWithError(quic.ApplicationErrorCode(quic.NoError), "client close")
}

func (svc *TPUService) IsConnecting(pk solana.PublicKey) bool {
	svc.mut.RLock()
	defer svc.mut.RUnlock()
	_, ok := svc.connecting[pk]
	return ok
}

func (svc *TPUService) SetConnecting(pk solana.PublicKey, connecting bool) {
	svc.mut.Lock()
	defer svc.mut.Unlock()
	if connecting {
		svc.connecting[pk] = struct{}{}
	} else {
		delete(svc.connecting, pk)
	}
}

func (svc *TPUService) IsConnected(pk solana.PublicKey) bool {
	_, ok := svc.pool.Peek(pk)
	return ok
}

func (svc *TPUService) Connect(ctx context.Context, leader *LeaderAtSlot) (*LeaderConn, error) {
	if conn, ok := svc.pool.Peek(leader.Info.Pubkey); ok {
		if conn.Conn.Context().Err() == nil {
			return conn, nil
		}
		svc.pool.Remove(leader.Info.Pubkey)
	}

	return svc.connect(ctx, leader)
}

func (svc *TPUService) connect(ctx context.Context, leader *LeaderAtSlot) (*LeaderConn, error) {
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

func (svc *TPUService) SendRawTransaction(ctx context.Context, rawTx []byte, ahead uint64, n int) error {
	leaders := svc.scheduler.FutureLeaders(ahead, n)
	var err error
	for _, leader := range leaders {
		err = svc.send(ctx, leader, rawTx)
		if err != nil {
			log.Error().Err(err).
				Uint64("current_slot", svc.scheduler.GetCurrentSlot()).
				Uint64("Slot", leader.Slot).
				Str("leader", leader.Info.Pubkey.String()).
				Msg("Failed to send to leader retry next leader")
		} else {
			log.Info().
				Uint64("current_slot", svc.scheduler.GetCurrentSlot()).
				Uint64("Slot", leader.Slot).
				Str("leader", leader.Info.Pubkey.String()).
				Msg("txn sent")
			break
		}
	}
	return nil
}

func (svc *TPUService) send(ctx context.Context, leader *LeaderAtSlot, rawTx []byte) error {
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
