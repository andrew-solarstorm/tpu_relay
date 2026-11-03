package tpu_relay

import (
	"context"
	"fmt"
	"sync"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/rs/zerolog/log"
)

type scheduler struct {
	rpcCli *rpc.Client

	mu           sync.RWMutex
	leaderAtSlot map[uint64]*Leader
	epSche       EpochSchedule
}

func newScheduler(
	ctx context.Context,
	rpcURL string,
) (*scheduler, error) {
	cli := rpc.New(rpcURL)

	scheduler := &scheduler{
		leaderAtSlot: make(map[uint64]*Leader),
		rpcCli:       cli,
	}

	err := scheduler.load(ctx)
	if err != nil {
		return nil, err
	}

	return scheduler, nil
}

// inEpoch reports whether slot falls before the end of the loaded epoch.
func (sche *scheduler) inEpoch(slot uint64) bool {
	sche.mu.RLock()
	defer sche.mu.RUnlock()
	return slot < sche.epSche.End && slot >= sche.epSche.Start
}

func (sche *scheduler) load(ctx context.Context) error {
	nodes, err := sche.rpcCli.GetClusterNodes(ctx)
	if err != nil {
		return fmt.Errorf("GetClusterNodes: %w", err)
	}
	log.Info().Int("nodes", len(nodes)).Msg("Total nodes from RPC")

	epoch, err := sche.rpcCli.GetEpochInfo(ctx, rpc.CommitmentProcessed)
	if err != nil {
		return fmt.Errorf("GetEpochInfo: %w", err)
	}

	leaderSchedule, err := sche.rpcCli.GetLeaderScheduleWithOpts(ctx, &rpc.GetLeaderScheduleOpts{
		Commitment: rpc.CommitmentProcessed,
		Epoch:      &epoch.AbsoluteSlot,
	})
	if err != nil {
		return fmt.Errorf("GetLeaderSchedule: %w", err)
	}
	log.Info().Int("leaders_in_schedule", len(leaderSchedule)).Msg("Leader schedule loaded")

	leaderAtSlot, epSche := buildIndex(nodes, epoch, leaderSchedule)

	sche.mu.Lock()
	sche.leaderAtSlot = leaderAtSlot
	sche.epSche = epSche
	sche.mu.Unlock()

	return nil
}

func buildIndex(
	nodes []*rpc.GetClusterNodesResult,
	epoch *rpc.GetEpochInfoResult,
	leaderSchedule rpc.GetLeaderScheduleResult,
) (map[uint64]*Leader, EpochSchedule) {
	leaders := make(map[solana.PK]*Leader, len(nodes))
	for _, node := range nodes {
		if node.TPUQUIC != nil && !node.Pubkey.IsZero() {
			leaders[node.Pubkey] = &Leader{
				TPUQUIC: *node.TPUQUIC,
				Pubkey:  node.Pubkey,
			}
		}
	}

	start := epoch.AbsoluteSlot - epoch.SlotIndex
	end := start + epoch.SlotsInEpoch

	leaderAtSlot := make(map[uint64]*Leader, epoch.SlotsInEpoch)
	for pk, slotIndices := range leaderSchedule {
		leader, ok := leaders[pk]
		if !ok {
			continue
		}

		for _, idx := range slotIndices {
			slot := start + idx
			if slot < epoch.AbsoluteSlot {
				continue
			}

			leaderAtSlot[slot] = leader
			log.Info().Uint64("slot", slot).Str("leader", pk.String()).Msg("building index")
		}
	}

	return leaderAtSlot, EpochSchedule{Start: start, End: end}
}

func (sche *scheduler) futureLeaders(slot, slotDiff, n uint64) []*LeaderAtSlot {
	if n == 0 {
		return nil
	}

	sche.mu.RLock()
	defer sche.mu.RUnlock()

	first := slot + slotDiff
	if first < slot { // overflow
		return nil
	}
	first = max(first, sche.epSche.Start)

	leaders := make([]*LeaderAtSlot, 0, min(n, 8))
	unique := make(map[solana.PK]struct{})

	for s := first; s < sche.epSche.End; s++ {
		l := sche.leaderAtSlot[s]
		if l == nil {
			continue
		}

		if _, ok := unique[l.Pubkey]; ok {
			continue
		}

		leaders = append(leaders, &LeaderAtSlot{Info: l, Slot: s})
		unique[l.Pubkey] = struct{}{}
		if uint64(len(leaders)) >= n {
			break
		}
	}

	return leaders
}
