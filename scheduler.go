package tpu_relay

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/rs/zerolog/log"
)

const prefetchSlots = 1000

var errNoNextSchedule = errors.New("rpc returned no leader schedule for the next epoch")

type scheduler struct {
	rpcCli *rpc.Client

	mu           sync.RWMutex
	leaderAtSlot map[uint64]*Leader
	epSche       EpochSchedule

	next     map[uint64]*Leader
	nextSche EpochSchedule
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
	sche.next = nil
	sche.mu.Unlock()

	return nil
}

func (sche *scheduler) loadNext(ctx context.Context) error {
	sche.mu.RLock()
	start := sche.epSche.End
	sche.mu.RUnlock()

	nodes, err := sche.rpcCli.GetClusterNodes(ctx)
	if err != nil {
		return fmt.Errorf("GetClusterNodes: %w", err)
	}

	schedule, err := sche.rpcCli.GetLeaderScheduleWithOpts(ctx, &rpc.GetLeaderScheduleOpts{
		Commitment: rpc.CommitmentProcessed,
		Epoch:      &start,
	})
	if err != nil {
		return fmt.Errorf("GetLeaderSchedule: %w", err)
	}
	if len(schedule) == 0 {
		return errNoNextSchedule
	}

	var slots uint64
	for _, indices := range schedule {
		for _, idx := range indices {
			slots = max(slots, idx+1)
		}
	}

	next := indexSchedule(nodeLeaders(nodes), schedule, start, 0)
	log.Info().Uint64("start", start).Uint64("slots", slots).Int("indexed", len(next)).Msg("Next leader schedule loaded")

	sche.mu.Lock()
	defer sche.mu.Unlock()
	if sche.epSche.End != start {
		return nil // the loaded epoch changed meanwhile, this schedule is for another one
	}
	sche.next = next
	sche.nextSche = EpochSchedule{Start: start, End: start + slots}
	return nil
}

func (sche *scheduler) needsNext(slot uint64) bool {
	sche.mu.RLock()
	defer sche.mu.RUnlock()
	return sche.next == nil && slot < sche.epSche.End && sche.epSche.End-slot <= prefetchSlots
}

func (sche *scheduler) advance(slot uint64) bool {
	sche.mu.Lock()
	defer sche.mu.Unlock()
	if sche.next == nil || slot < sche.nextSche.Start || slot >= sche.nextSche.End {
		return false
	}
	sche.leaderAtSlot, sche.epSche = sche.next, sche.nextSche
	sche.next = nil
	return true
}

func buildIndex(
	nodes []*rpc.GetClusterNodesResult,
	epoch *rpc.GetEpochInfoResult,
	leaderSchedule rpc.GetLeaderScheduleResult,
) (map[uint64]*Leader, EpochSchedule) {
	start := epoch.AbsoluteSlot - epoch.SlotIndex
	end := start + epoch.SlotsInEpoch

	leaderAtSlot := indexSchedule(nodeLeaders(nodes), leaderSchedule, start, epoch.AbsoluteSlot)
	log.Info().Int("slots", len(leaderAtSlot)).Msg("leader index built")

	return leaderAtSlot, EpochSchedule{Start: start, End: end}
}

// nodeLeaders returns the nodes that can be dialled over QUIC, by identity.
func nodeLeaders(nodes []*rpc.GetClusterNodesResult) map[solana.PK]*Leader {
	leaders := make(map[solana.PK]*Leader, len(nodes))
	for _, node := range nodes {
		if node.TPUQUIC != nil && !node.Pubkey.IsZero() {
			leaders[node.Pubkey] = &Leader{
				TPUQUIC: *node.TPUQUIC,
				Pubkey:  node.Pubkey,
			}
		}
	}
	return leaders
}

func indexSchedule(
	known map[solana.PK]*Leader,
	schedule rpc.GetLeaderScheduleResult,
	start, from uint64,
) map[uint64]*Leader {
	var total int
	for _, slotIndices := range schedule {
		total += len(slotIndices)
	}

	leaderAtSlot := make(map[uint64]*Leader, total)
	for pk, slotIndices := range schedule {
		leader, ok := known[pk]
		if !ok {
			continue
		}

		for _, idx := range slotIndices {
			if slot := start + idx; slot >= from {
				leaderAtSlot[slot] = leader
			}
		}
	}
	return leaderAtSlot
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

	end := sche.epSche.End
	if sche.next != nil {
		end = sche.nextSche.End
	}

	leaders := make([]*LeaderAtSlot, 0, min(n, 8))
	unique := make(map[solana.PK]struct{})

	for s := first; s < end; s++ {
		l := sche.leaderAt(s)
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

func (sche *scheduler) leaderAt(slot uint64) *Leader {
	if slot < sche.epSche.End {
		return sche.leaderAtSlot[slot]
	}
	return sche.next[slot]
}
