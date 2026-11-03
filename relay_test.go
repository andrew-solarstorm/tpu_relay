package tpu_relay

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go/rpc"
)

func TestSendRawTransactionErrors(t *testing.T) {
	// neither case reaches the TPU pool, so r.tpu can stay nil
	r := &Relay{schler: newTestScheduler(100, 200, map[uint64]byte{150: 1})}

	if err := r.SendRawTransaction(context.Background(), []byte("tx"), 0, 2); !errors.Is(err, ErrNoSlot) {
		t.Errorf("before the first slot: got %v, want ErrNoSlot", err)
	}

	// current slot is past the last known leader
	r.checkAndSetSlot(160)
	if err := r.SendRawTransaction(context.Background(), []byte("tx"), 0, 2); !errors.Is(err, ErrNoLeaders) {
		t.Errorf("no leaders ahead: got %v, want ErrNoLeaders", err)
	}
}

// fakeRPC is a JSON-RPC server for the three calls scheduler.load makes. The
// first failNodes getClusterNodes calls fail with a 500, and the first
// staleEpochs getEpochInfo calls report an epoch that has already ended.
type fakeRPC struct {
	failNodes, staleEpochs int32
	nodeCalls, epochCalls  atomic.Int32
}

func (f *fakeRPC) serve(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var in struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var result any
		switch in.Method {
		case "getClusterNodes":
			if f.nodeCalls.Add(1) <= f.failNodes {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
			result = []map[string]any{{"pubkey": pk(1).String(), "tpuQuic": "127.0.0.1:8009"}}
		case "getEpochInfo":
			epoch := map[string]any{"absoluteSlot": 1005, "slotIndex": 5, "slotsInEpoch": 100}
			if f.epochCalls.Add(1) <= f.staleEpochs {
				epoch = map[string]any{"absoluteSlot": 950, "slotIndex": 50, "slotsInEpoch": 100} // ends at 1000
			}
			result = epoch
		case "getLeaderSchedule":
			result = map[string][]uint64{pk(1).String(): {5, 6}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": in.ID, "result": result})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// newReloadRelay returns a relay whose scheduler has no epoch loaded yet, with
// the current slot already past anything that could be loaded from before it.
func newReloadRelay(ctx context.Context, url string) *Relay {
	r := &Relay{
		ctx: ctx,
		schler: &scheduler{
			rpcCli:       rpc.New(url),
			leaderAtSlot: map[uint64]*Leader{},
		},
	}
	r.checkAndSetSlot(1005)
	return r
}

func fastReloads(t *testing.T) {
	t.Helper()
	oldDelay, oldMax := reloadDelay, reloadMaxDelay
	reloadDelay, reloadMaxDelay = 5*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { reloadDelay, reloadMaxDelay = oldDelay, oldMax })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestReloadScheduleRetriesUntilLoaded(t *testing.T) {
	fastReloads(t)
	f := &fakeRPC{failNodes: 2, staleEpochs: 1}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newReloadRelay(ctx, f.serve(t).URL)

	// repeated triggers while one reload is running must not start more
	for range 5 {
		r.reloadSchedule()
	}

	waitFor(t, "schedule to load", func() bool { return r.schler.inEpoch(1005) && !r.reloading.Load() })

	// 2 failed + 1 stale + 1 good
	if n := f.nodeCalls.Load(); n != 4 {
		t.Errorf("getClusterNodes called %d times, want 4", n)
	}
	if got := r.schler.futureLeaders(1005, 0, 1); len(got) != 1 || got[0].Slot != 1005 {
		t.Errorf("loaded index has leaders %v, want one at slot 1005", slotsOf(got))
	}
}

func TestReloadScheduleStopsWhenContextDone(t *testing.T) {
	fastReloads(t)
	f := &fakeRPC{failNodes: 1 << 30} // never succeeds
	ctx, cancel := context.WithCancel(context.Background())
	r := newReloadRelay(ctx, f.serve(t).URL)

	r.reloadSchedule()
	waitFor(t, "first retry", func() bool { return f.nodeCalls.Load() >= 2 })

	cancel()
	waitFor(t, "reload to stop", func() bool { return !r.reloading.Load() })
}
