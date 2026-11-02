package tpu_relay

import (
	"context"
	"sync"

	"github.com/rs/zerolog/log"
)

type Relay struct {
	tpu    *tpuDiscover
	schler *scheduler

	slotCh chan uint64
	ctx    context.Context

	crsMu  sync.RWMutex
	crSlot uint64
}

func NewRelay(ctx context.Context, rpcURL string, maxCon int) (*Relay, error) {
	tpu, err := NewTPUDiscover(maxCon)
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
	for slot := range r.slotCh {
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		if !r.checkAndSetSlot(slot) {
			continue
		}

		if !r.schler.inEpoch(slot) {
			err := r.schler.load(r.ctx)
			log.Error().Err(err).Msg("failed to load schedule")
			continue
		}

		leaders := r.schler.futureLeaders(slot, 1, 3)
		go func() {
			for _, leader := range leaders {
				if !r.tpu.IsConnected(leader.Info.Pubkey) && !r.tpu.IsConnecting(leader.Info.Pubkey) {
					_, err := r.tpu.Connect(r.ctx, leader)
					if err != nil {
						log.Error().Err(err).Msgf("failed to establish connection to %s at slot %d", leader.Info.Pubkey.String(), leader.Slot)
					}
				}
			}
		}()

	}
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
	leaders := r.schler.futureLeaders(r.CurrentSlot(), ahead, n)
	var err error
	for _, leader := range leaders {
		err = r.tpu.Send(ctx, leader, rawTx)
		if err != nil {
			log.Error().Err(err).
				Uint64("current_slot", r.CurrentSlot()).
				Uint64("Slot", leader.Slot).
				Str("leader", leader.Info.Pubkey.String()).
				Msg("Failed to send to leader retry next leader")
		} else {
			log.Info().
				Uint64("current_slot", r.CurrentSlot()).
				Uint64("Slot", leader.Slot).
				Str("leader", leader.Info.Pubkey.String()).
				Msg("txn sent")
			break
		}
	}
	return nil
}

func (r *Relay) SendTransaction(
	ctx context.Context,
	rawTx []byte,
	ahead uint64,
	n uint64,
) error {
	return nil
}
