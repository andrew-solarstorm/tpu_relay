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

// errStaleEpoch means the RPC answered, but still serves the previous epoch.
var errStaleEpoch = errors.New("rpc is still serving the previous epoch")

// reload retry delays grow exponentially from reloadDelay up to reloadMaxDelay.
// They are variables so tests can shorten them.
var (
	reloadDelay    = 500 * time.Millisecond
	reloadMaxDelay = 10 * time.Second
)

type Relay struct {
	tpu    *tpuDiscover
	schler *scheduler

	slotCh chan uint64
	ctx    context.Context

	crsMu  sync.RWMutex
	crSlot uint64

	// reloading is set while a background schedule reload is running.
	reloading atomic.Bool
}

// NewRelay starts a relay that runs until ctx is cancelled, at which point
// every pooled connection is closed.
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

	if !r.schler.inEpoch(slot) {
		r.reloadSchedule()
		return
	}

	leaders := r.schler.futureLeaders(slot, 1, 3)
	go func() {
		for _, leader := range leaders {
			log.Info().Uint64("slot", leader.Slot).Str("leader", leader.Info.Pubkey.String()).Str("name", leader.Info.QUIC).Msg("info")
			// Connect is a no-op for a live connection and shares an in-flight dial.
			_, err := r.tpu.Connect(r.ctx, leader)
			if err != nil {
				log.Error().Err(err).Msgf("failed to establish connection to %s at slot %d", leader.Info.Pubkey.String(), leader.Slot)
			}
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

// SendRawTransaction sends rawTx to the first of the next n distinct leaders
// (starting ahead slots from the current slot) that accepts it. It returns an
// error if no leader could be found or every attempt failed.
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

	var errs []error
	for _, leader := range leaders {
		err := r.tpu.Send(ctx, leader, rawTx)
		if err == nil {
			log.Info().
				Uint64("current_slot", slot).
				Uint64("Slot", leader.Slot).
				Str("leader", leader.Info.Pubkey.String()).
				Msg("txn sent")
			return nil
		}

		log.Error().Err(err).
			Uint64("current_slot", slot).
			Uint64("Slot", leader.Slot).
			Str("leader", leader.Info.Pubkey.String()).
			Msg("Failed to send to leader retry next leader")
		errs = append(errs, fmt.Errorf("leader %s at slot %d: %w", leader.Info.Pubkey, leader.Slot, err))
	}
	return errors.Join(errs...)
}
