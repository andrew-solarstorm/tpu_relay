package tpu_relay

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go/v5"
	"github.com/rs/zerolog/log"
)

var (
	ErrNoSlot         = errors.New("tpu_relay: no slot received yet")
	ErrNoLeaders      = errors.New("tpu_relay: no upcoming leaders in the loaded schedule")
	ErrNotImplemented = errors.New("tpu_relay: not implemented")
)

var errStaleEpoch = errors.New("rpc is still serving the previous epoch")

var (
	reloadDelay    = 500 * time.Millisecond
	reloadMaxDelay = 10 * time.Second
)

var sendAttemptTimeout = time.Second

type Relay struct {
	tpu    *tpuDiscover
	schler *scheduler

	slotCh chan uint64
	ctx    context.Context

	crsMu  sync.RWMutex
	crSlot uint64

	reloading   atomic.Bool
	prefetching atomic.Bool
}

func NewRelay(ctx context.Context, rpcURL string, maxCon int) (*Relay, error) {
	tpu, err := NewTPUDiscover(ctx, maxCon)
	if err != nil {
		return nil, err
	}
	schler, err := newScheduler(ctx, rpcURL)
	if err != nil {
		return nil, err
	}
	r := Relay{
		tpu:    tpu,
		schler: schler,
		slotCh: make(chan uint64, 30),
		ctx:    ctx,
	}
	go r.worker()
	return &r, nil
}

func (r *Relay) worker() {
	defer r.tpu.Close()

	for {
		select {
		case <-r.ctx.Done():
			return
		case slot := <-r.slotCh:
			r.handleSlot(slot)
		}
	}
}

func (r *Relay) handleSlot(slot uint64) {
	if !r.checkAndSetSlot(slot) {
		return
	}

	// the prefetched schedule makes the epoch switch free; reload only if it is missing
	if !r.schler.inEpoch(slot) && !r.schler.advance(slot) {
		r.reloadSchedule()
		return
	}

	if r.schler.needsNext(slot) {
		r.prefetchSchedule(slot)
	}

	leaders := r.schler.futureLeaders(slot, 1, 3)
	for _, leader := range leaders {
		go func() {
			_, err := r.tpu.Connect(r.ctx, leader)
			if err != nil {
				// connect already warns with the dial details; this is the pre-connect for an upcoming leader
				log.Debug().Err(err).Msgf("failed to establish connection to %s at slot %d", leader.Leader.Pubkey.String(), leader.Slot)
			}
		}()
	}
}

func (r *Relay) prefetchSchedule(slot uint64) {
	if !r.prefetching.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer r.prefetching.Store(false)

		err := retry.New(
			retry.Context(r.ctx),
			retry.UntilSucceeded(),
			retry.Delay(reloadDelay),
			retry.MaxDelay(reloadMaxDelay),
			retry.LastErrorOnly(true),
			retry.OnRetry(func(attempt uint, err error) {
				log.Warn().Err(err).Uint("attempt", attempt).Msg("failed to prefetch next schedule, retrying")
			}),
		).Do(func() error {
			if !r.schler.inEpoch(r.CurrentSlot()) {
				return retry.Unrecoverable(errors.New("epoch ended before the next schedule loaded"))
			}
			return r.schler.loadNext(r.ctx)
		})
		if err != nil && r.ctx.Err() == nil {
			log.Error().Err(err).Uint64("slot", slot).Msg("giving up on prefetching next schedule")
		}
	}()
}

func (r *Relay) reloadSchedule() {
	if !r.reloading.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer r.reloading.Store(false)

		err := retry.New(
			retry.Context(r.ctx),
			retry.UntilSucceeded(),
			retry.Delay(reloadDelay),
			retry.MaxDelay(reloadMaxDelay),
			retry.LastErrorOnly(true),
			retry.OnRetry(func(attempt uint, err error) {
				log.Warn().Err(err).Uint("attempt", attempt).Msg("failed to load schedule, retrying")
			}),
		).Do(func() error {
			if err := r.schler.load(r.ctx); err != nil {
				return err
			}
			// the RPC can lag behind the slot feed and still serve the old epoch
			if !r.schler.inEpoch(r.CurrentSlot()) {
				return errStaleEpoch
			}
			return nil
		})
		if err != nil && r.ctx.Err() == nil {
			log.Error().Err(err).Msg("giving up on loading schedule")
		}
	}()
}

func (r *Relay) OnSlot(slot uint64) {
	select {
	case r.slotCh <- slot:
	default:
		log.Warn().Int("len_slot_ch", len(r.slotCh)).Msg("slot ch is too full")
	}
}

func (r *Relay) checkAndSetSlot(slot uint64) bool {
	r.crsMu.Lock()
	defer r.crsMu.Unlock()
	if r.crSlot < slot {
		r.crSlot = slot
		return true
	}
	return false
}

func (r *Relay) CurrentSlot() uint64 {
	r.crsMu.RLock()
	defer r.crsMu.RUnlock()
	return r.crSlot
}

func (r *Relay) SendRawTransaction(
	ctx context.Context,
	rawTx []byte,
	ahead uint64,
	n uint64,
) error {
	slot := r.CurrentSlot()
	if slot == 0 {
		return ErrNoSlot
	}

	leaders := r.schler.futureLeaders(slot, ahead, n)
	if len(leaders) == 0 {
		return ErrNoLeaders
	}

	sendCtx := context.WithoutCancel(ctx)
	results := make(chan error, len(leaders))
	for _, leader := range leaders {
		go func() { results <- r.sendToLeader(sendCtx, slot, leader, rawTx) }()
	}

	var errs []error
	for range leaders {
		select {
		case err := <-results:
			if err == nil {
				return nil
			}
			errs = append(errs, err)
		case <-ctx.Done():
			return errors.Join(append(errs, ctx.Err())...)
		}
	}
	return errors.Join(errs...)
}

func (r *Relay) sendToLeader(ctx context.Context, slot uint64, leader *LeaderAtSlot, rawTx []byte) error {
	ctx, cancel := context.WithTimeout(ctx, sendAttemptTimeout)
	defer cancel()

	l := log.With().
		Uint64("current_slot", slot).
		Uint64("Slot", leader.Slot).
		Str("quic", leader.Leader.TPUQUIC).
		Str("leader", leader.Leader.Pubkey.String()).
		Str("Name", leader.Leader.Name).
		Str("country", leader.Leader.IPCountry).
		Str("city", leader.Leader.IPCity).
		Bool("is JITO", leader.Leader.IsJITO).
		Str("client", leader.Leader.Client).
		Uint64("slot", leader.Slot).
		Logger()

	if err := r.tpu.Send(ctx, leader, rawTx); err != nil {
		// one leader failing is expected, SendRawTransaction only errors once all of them do
		l.Warn().Err(err).Msg("Failed to send to leader")
		return fmt.Errorf("leader %s at slot %d: %w", leader.Leader.Pubkey, leader.Slot, err)
	}
	l.Info().Msg("txn sent")
	return nil
}
