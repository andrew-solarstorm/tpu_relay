package tpu_relay

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/andrew-solarstorm/digo"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/rs/zerolog/log"
)

const SCHEDULER_SVC = "leader-schedule-service"

// ScheduleService will monitor for leader, and slot and answer
// What's leader for a given slot?
type ScheduleService struct {
	digo.BaseDIInstance

	config *Config
	rpcCli *rpc.Client

	mu    sync.RWMutex
	epoch *rpc.GetEpochInfoResult

	// map pk -> Leader
	leaders map[solana.PublicKey]*Leader

	// map slot -> Leader
	slotToLeader map[uint64]*Leader

	currentSlot atomic.Uint64
	slotCh      chan uint64

	ctx       context.Context
	ctxCancel context.CancelFunc

	tpuSvc *TPUService
}

func (svc *ScheduleService) ID() string {
	return SCHEDULER_SVC
}

func (svc *ScheduleService) Configure(c digo.IContainer) error {
	conf := digo.GetConfig[*Config](c)
	svc.config = conf
	svc.rpcCli = rpc.New(svc.config.RPCUrl)
	svc.leaders = make(map[solana.PublicKey]*Leader)
	svc.slotCh = make(chan uint64, 10)
	svc.slotToLeader = make(map[uint64]*Leader)

	svc.tpuSvc = digo.Instance[*TPUService](TPU_SVC, c)
	return nil
}

func (svc *ScheduleService) Start() error {
	svc.ctx, svc.ctxCancel = context.WithCancel(context.Background())

	err := svc.load(context.Background())
	if err != nil {
		return err
	}
	go svc.worker()
	return nil
}

func (svc *ScheduleService) Stop() error {
	if svc.ctxCancel != nil {
		svc.ctxCancel()
	}
	return nil
}

func (svc *ScheduleService) load(ctx context.Context) error {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	nodes, err := svc.rpcCli.GetClusterNodes(ctx)
	if err != nil {
		return fmt.Errorf("GetClusterNodes: %w", err)
	}
	log.Info().Int("nodes", len(nodes)).Msg("Total nodes from RPC")

	epoch, err := svc.rpcCli.GetEpochInfo(ctx, rpc.CommitmentProcessed)
	if err != nil {
		return fmt.Errorf("GetEpochInfo: %w", err)
	}
	svc.epoch = epoch

	schedule, err := svc.rpcCli.GetLeaderSchedule(ctx)
	if err != nil {
		return fmt.Errorf("GetLeaderSchedule: %w", err)
	}
	log.Info().Int("leaders_in_schedule", len(schedule)).Msg("Leader schedule loaded")

	svc.buildIndex(nodes, epoch, schedule)

	return nil
}

func (svc *ScheduleService) worker() {
	for {
		select {
		case <-svc.ctx.Done():
			return
		case slot := <-svc.slotCh:
			if slot <= svc.currentSlot.Load() {
				continue
			}

			svc.mu.RLock()
			epochSlotsInEpoch := svc.epoch.SlotsInEpoch
			slotIdx := svc.idxOfSlotLocked(slot)
			svc.mu.RUnlock()

			if slotIdx > (epochSlotsInEpoch - 3) {
				err := svc.load(context.Background())
				if err != nil {
					log.Error().Err(err).Msg("failed to load")
				}
			}

			svc.currentSlot.Store(slot)
			leaders := svc.FutureLeaders(1, 3)
			go func() {
				svc.preConnect(leaders)
			}()
		}
	}
}

func (svc *ScheduleService) preConnect(leaders []*LeaderAtSlot) {
	for _, leader := range leaders {
		if svc.tpuSvc.IsConnected(leader.Info.Pubkey) || svc.tpuSvc.IsConnecting(leader.Info.Pubkey) {
			continue
		}
		log.Info().Uint64("Slot", leader.Slot).Str("pubkey", leader.Info.Pubkey.String()).Msg("Preconnect to leader")
		svc.tpuSvc.Connect(context.Background(), leader)
	}
}

func (svc *ScheduleService) buildIndex(
	nodes []*rpc.GetClusterNodesResult,
	epoch *rpc.GetEpochInfoResult,
	schedule rpc.GetLeaderScheduleResult,
) {
	// build nodes map
	for _, node := range nodes {
		if node.TPUQUIC != nil && !node.Pubkey.IsZero() {
			svc.leaders[node.Pubkey] = &Leader{
				TPUQUIC: *node.TPUQUIC,
				Pubkey:  node.Pubkey,
			}
		}
	}

	// slot -> leader map
	for pk, slotIndexes := range schedule {
		for _, idx := range slotIndexes {
			slot := svc.slotAtIdxLocked(idx)
			if slot < epoch.AbsoluteSlot {
				continue
			}
			if _, ok := svc.leaders[pk]; !ok {
			} else {
				svc.slotToLeader[slot] = svc.leaders[pk]
			}
		}
	}
}

func (svc *ScheduleService) OnSlot(slot uint64) {
	select {
	case svc.slotCh <- slot:
	default:
		log.Warn().Int("len_slot_ch", len(svc.slotCh)).Msg("slot ch is too full")
	}
}

func (svc *ScheduleService) FutureLeaders(slotDiff uint64, n int) []*LeaderAtSlot {
	leaders := make([]*LeaderAtSlot, 0, n)
	unique := make(map[solana.PublicKey]struct{})

	currentSlot := svc.currentSlot.Load()
	i := 0
	for {
		l := svc.LeaderAt(currentSlot + slotDiff + uint64(i))
		i++
		if l == nil {
			continue
		}
		if _, ok := unique[l.Pubkey]; ok {
			continue
		}

		leaders = append(leaders, &LeaderAtSlot{
			Slot: currentSlot + slotDiff + uint64(i),
			Info: l,
		})
		unique[l.Pubkey] = struct{}{}

		if len(unique) >= n {
			break
		}
	}
	return leaders
}

func (svc *ScheduleService) CurrentLeader() *Leader {
	return svc.LeaderAt(svc.currentSlot.Load())
}

func (svc *ScheduleService) LeaderAt(slot uint64) *Leader {
	svc.mu.RLock()
	defer svc.mu.RUnlock()
	leader := svc.slotToLeader[slot]
	if leader == nil {
		log.Warn().
			Uint64("slot", slot).
			Uint64("current_slot", svc.currentSlot.Load()).
			Int("schedule_size", len(svc.slotToLeader)).
			Msg("Leader not found for slot - schedule may be incomplete or stale")
	}
	return leader
}

func (svc *ScheduleService) GetCurrentSlot() uint64 {
	return svc.currentSlot.Load()
}

func (svc *ScheduleService) idxOfSlotLocked(slot uint64) uint64 {
	absSlot := svc.epoch.AbsoluteSlot
	slotIndex := svc.epoch.SlotIndex
	if slot < absSlot {
		return 0
	}
	return (slot - absSlot) + slotIndex
}

func (svc *ScheduleService) slotAtIdxLocked(index uint64) uint64 {
	absSlot := svc.epoch.AbsoluteSlot
	slotIndex := svc.epoch.SlotIndex
	if index < slotIndex {
		return 0
	}
	return absSlot + (index - slotIndex)
}
