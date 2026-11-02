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
	leaders      map[solana.PK]*Leader
	leaderAtSlot map[uint64]*Leader

	epSche *EpochSchedule
}

func newScheduler(
	ctx context.Context,
	rpcURL string,
) (*scheduler, error) {
	cli := rpc.New(rpcURL)

	scheduler := &scheduler{
		leaders:      make(map[solana.PK]*Leader),
		leaderAtSlot: make(map[uint64]*Leader),
		rpcCli:       cli,
	}

	err := scheduler.load(ctx)
	if err != nil {
		return nil, err
	}

	return scheduler, nil
}

func (sch *scheduler) inEpoch(slot uint64) bool {
	return slot < sch.epSche.End-1
}

func (sche *scheduler) load(ctx context.Context) error {
	sche.mu.Lock()
	defer sche.mu.Unlock()

	nodes, err := sche.rpcCli.GetClusterNodes(ctx)
	if err != nil {
		return fmt.Errorf("GetClusterNodes: %w", err)
	}
	log.Info().Int("nodes", len(nodes)).Msg("Total nodes from RPC")

	epoch, err := sche.rpcCli.GetEpochInfo(ctx, rpc.CommitmentProcessed)
	if err != nil {
		return fmt.Errorf("GetEpochInfo: %w", err)
	}

	leaderSchedule, err := sche.rpcCli.GetLeaderSchedule(ctx)
	if err != nil {
		return fmt.Errorf("GetLeaderSchedule: %w", err)
	}
	log.Info().Int("leaders_in_schedule", len(leaderSchedule)).Msg("Leader schedule loaded")

	sche.buildIndex(nodes, epoch, leaderSchedule)

	return nil
}

func (sche *scheduler) buildIndex(
	nodes []*rpc.GetClusterNodesResult,
	epoch *rpc.GetEpochInfoResult,
	leaderSchedule rpc.GetLeaderScheduleResult,
) {
	for _, node := range nodes {
		if node.TPUQUIC != nil && !node.Pubkey.IsZero() {
			sche.leaders[node.Pubkey] = &Leader{
				TPUQUIC: *node.TPUQUIC,
				Pubkey:  node.Pubkey,
			}
		}
	}

	start := epoch.AbsoluteSlot - epoch.SlotIndex
	end := start + epoch.SlotsInEpoch

	sche.epSche = &EpochSchedule{
		Start: start,
		End:   end,
	}

	for pk, slotIndices := range leaderSchedule {
		for _, idx := range slotIndices {
			slot := start + idx
			if slot < epoch.AbsoluteSlot {
				continue
			}

			if _, ok := sche.leaders[pk]; ok {
				sche.leaderAtSlot[slot] = sche.leaders[pk]
			}
		}
	}
}

func (sche *scheduler) leaderAt(slot uint64) *Leader {
	sche.mu.RLock()
	defer sche.mu.RUnlock()
	return sche.leaderAtSlot[slot]
}

func (sche *scheduler) futureLeaders(slot, slotDiff, n uint64) []*LeaderAtSlot {
	leaders := make([]*LeaderAtSlot, 0, n)
	unique := make(map[solana.PK]struct{})

	i := slotDiff

	for {
		l := sche.leaderAt(slot + i)
		i++

		if l == nil {
			continue
		}

		if _, ok := unique[l.Pubkey]; ok {
			continue
		}

		leaders = append(leaders, &LeaderAtSlot{Info: l, Slot: slot + i})
		unique[l.Pubkey] = struct{}{}
		if len(unique) >= int(n) {
			break
		}
	}

	return leaders
}
