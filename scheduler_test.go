package tpu_relay

import (
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

func pk(b byte) solana.PublicKey {
	var k solana.PublicKey
	k[0] = b
	return k
}

// newTestScheduler builds a scheduler for the epoch [start, end) where
// bySlot maps a slot to the id of its leader.
func newTestScheduler(start, end uint64, bySlot map[uint64]byte) *scheduler {
	leaders := map[byte]*Leader{}
	leaderAtSlot := map[uint64]*Leader{}
	for slot, id := range bySlot {
		l, ok := leaders[id]
		if !ok {
			l = &Leader{Pubkey: pk(id)}
			leaders[id] = l
		}
		leaderAtSlot[slot] = l
	}
	return &scheduler{
		leaderAtSlot: leaderAtSlot,
		epSche:       EpochSchedule{Start: start, End: end},
	}
}

func slotsOf(ls []*LeaderAtSlot) []uint64 {
	out := make([]uint64, len(ls))
	for i, l := range ls {
		out[i] = l.Slot
	}
	return out
}

func TestFutureLeadersReportsLeaderSlot(t *testing.T) {
	s := newTestScheduler(100, 200, map[uint64]byte{101: 1, 102: 2})

	got := s.futureLeaders(100, 1, 1)
	if len(got) != 1 || got[0].Slot != 101 || got[0].Info.Pubkey != pk(1) {
		t.Fatalf("want leader 1 at slot 101, got %v", slotsOf(got))
	}
}

func TestFutureLeadersSkipsDuplicatesAndGaps(t *testing.T) {
	s := newTestScheduler(100, 200, map[uint64]byte{
		101: 1, 102: 1, 103: 1, 104: 1, // leader 1 owns four slots
		105: 2, 106: 2,
		108: 3, // 107 has no known leader
	})

	got := s.futureLeaders(100, 1, 3)
	if want := []uint64{101, 105, 108}; !equal(slotsOf(got), want) {
		t.Fatalf("want slots %v, got %v", want, slotsOf(got))
	}
}

func TestFutureLeadersTerminatesAtEpochEnd(t *testing.T) {
	// only two distinct leaders remain before the epoch ends, but three are asked for
	s := newTestScheduler(100, 104, map[uint64]byte{100: 1, 101: 1, 102: 2, 103: 2})

	done := make(chan []*LeaderAtSlot, 1)
	go func() { done <- s.futureLeaders(99, 1, 3) }()

	select {
	case got := <-done:
		if want := []uint64{100, 102}; !equal(slotsOf(got), want) {
			t.Fatalf("want slots %v, got %v", want, slotsOf(got))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("futureLeaders did not return near the end of the epoch")
	}
}

func TestFutureLeadersEdgeCases(t *testing.T) {
	s := newTestScheduler(1000, 1100, map[uint64]byte{1000: 1, 1001: 2})

	if got := s.futureLeaders(1000, 0, 0); len(got) != 0 {
		t.Errorf("n=0: want none, got %v", slotsOf(got))
	}
	// a slot from before the epoch must start at the epoch, not scan from 0
	if got := s.futureLeaders(0, 0, 2); !equal(slotsOf(got), []uint64{1000, 1001}) {
		t.Errorf("slot 0: got %v", slotsOf(got))
	}
	if got := s.futureLeaders(^uint64(0), 5, 2); len(got) != 0 {
		t.Errorf("overflow: want none, got %v", slotsOf(got))
	}
	// n larger than any preallocation must not blow up
	if got := s.futureLeaders(1000, 0, 1<<60); len(got) != 2 {
		t.Errorf("huge n: want 2 leaders, got %v", slotsOf(got))
	}
}

func TestInEpochIsExclusiveOfEnd(t *testing.T) {
	s := newTestScheduler(100, 200, nil)
	if !s.inEpoch(199) {
		t.Error("last slot of the epoch should be in the epoch")
	}
	if s.inEpoch(200) {
		t.Error("first slot of the next epoch should not be in the epoch")
	}
}

func TestBuildIndex(t *testing.T) {
	quic := func(s string) *string { return &s }
	nodes := []*rpc.GetClusterNodesResult{
		{Pubkey: pk(1), TPUQUIC: quic("1.1.1.1:8009")},
		{Pubkey: pk(2)}, // no TPU QUIC address, can't be dialled
	}
	epoch := &rpc.GetEpochInfoResult{AbsoluteSlot: 1005, SlotIndex: 5, SlotsInEpoch: 100}
	schedule := rpc.GetLeaderScheduleResult{
		pk(1): {3, 5, 6}, // slot 1003 is already in the past
		pk(2): {7},
		pk(3): {8}, // not a known node
	}

	idx, ep := buildIndex(nodes, epoch, schedule)

	if ep.Start != 1000 || ep.End != 1100 {
		t.Errorf("epoch = %+v, want {1000 1100}", ep)
	}
	if len(idx) != 2 || idx[1005] == nil || idx[1006] == nil {
		t.Fatalf("index has %d entries, want slots 1005 and 1006 only", len(idx))
	}
	if idx[1005].Pubkey != pk(1) || idx[1005].TPUQUIC != "1.1.1.1:8009" {
		t.Errorf("unexpected leader at 1005: %+v", idx[1005])
	}
}

func equal(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
