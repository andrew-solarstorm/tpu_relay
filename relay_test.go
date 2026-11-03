package tpu_relay

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestLoadNextAppendsFollowingEpoch(t *testing.T) {
	f := &fakeRPC{}
	s := &scheduler{
		rpcCli:       rpc.New(f.serve(t).URL),
		leaderAtSlot: map[uint64]*Leader{},
		epSche:       EpochSchedule{Start: 1000, End: 1100},
	}

	if err := s.loadNext(context.Background()); err != nil {
		t.Fatal(err)
	}

	// the fake schedule gives pk(1) epoch indices 5 and 6
	if s.nextSche.Start != 1100 || s.nextSche.End != 1107 {
		t.Errorf("next epoch = %+v, want {1100 1107}", s.nextSche)
	}
	if s.next[1105] == nil || s.next[1106] == nil || len(s.next) != 2 {
		t.Errorf("next index has %d entries, want slots 1105 and 1106", len(s.next))
	}
	if got := s.futureLeaders(1099, 0, 1); len(got) != 1 || got[0].Slot != 1105 {
		t.Errorf("leaders across the boundary = %v, want slot 1105", slotsOf(got))
	}
}

// blackhole returns the address of a UDP socket that never answers.
func blackhole(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot listen on loopback UDP: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn.LocalAddr().String()
}

// newTPURelay returns a relay in the middle of a long epoch whose leaders are
// the given TPU addresses, one per slot starting at slot 101.
func newTPURelay(t *testing.T, ctx context.Context, addrs ...string) *Relay {
	t.Helper()
	tpu, err := NewTPUDiscover(ctx, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tpu.Close)

	leaderAtSlot := map[uint64]*Leader{}
	for i, addr := range addrs {
		leaderAtSlot[101+uint64(i)] = &Leader{Pubkey: pk(byte(i + 1)), TPUQUIC: addr}
	}
	return &Relay{
		ctx:    ctx,
		tpu:    tpu,
		schler: &scheduler{leaderAtSlot: leaderAtSlot, epSche: EpochSchedule{Start: 100, End: 100 + 10*prefetchSlots}},
	}
}

func TestPreConnectDoesNotWaitOnSlowLeader(t *testing.T) {
	good, accepted, _ := startTPUServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newTPURelay(t, ctx, blackhole(t), good)

	start := time.Now()
	r.handleSlot(100)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("handleSlot blocked for %s on a dial", took)
	}

	// the first leader never completes its handshake; the second must not wait for it
	waitFor(t, "connection to the second leader", func() bool { return accepted.Load() == 1 })
}

func TestEpochSwitchUsesPrefetchedSchedule(t *testing.T) {
	f := &fakeRPC{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tpu, err := NewTPUDiscover(ctx, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer tpu.Close()
	r := &Relay{
		ctx: ctx,
		tpu: tpu,
		schler: &scheduler{
			rpcCli:       rpc.New(f.serve(t).URL),
			leaderAtSlot: map[uint64]*Leader{},
			epSche:       EpochSchedule{Start: 1000, End: 1100},
		},
	}

	// inside the prefetch window, the next epoch loads in the background
	r.handleSlot(1050)
	waitFor(t, "next schedule", func() bool {
		r.schler.mu.RLock()
		defer r.schler.mu.RUnlock()
		return r.schler.next != nil
	})

	// crossing the boundary switches over; a reload would have put the epoch
	// back to [1000, 1100), the only one the fake RPC serves
	r.handleSlot(1100)
	time.Sleep(50 * time.Millisecond)
	if !r.schler.inEpoch(1100) || r.schler.inEpoch(1050) {
		t.Fatalf("epoch after the boundary = %+v", r.schler.epSche)
	}
	if got := r.schler.futureLeaders(1100, 0, 1); len(got) != 1 || got[0].Slot != 1105 {
		t.Errorf("leaders after the switch = %v, want slot 1105", slotsOf(got))
	}
}

func TestSendDeliversToEveryLeader(t *testing.T) {
	a, _, aPayloads := startTPUServer(t)
	b, _, bPayloads := startTPUServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newTPURelay(t, ctx, a, b)
	r.checkAndSetSlot(100)

	if err := r.SendRawTransaction(ctx, []byte("tx"), 1, 2); err != nil {
		t.Fatal(err)
	}

	for name, payloads := range map[string]chan []byte{"first": aPayloads, "second": bPayloads} {
		select {
		case got := <-payloads:
			if string(got) != "tx" {
				t.Fatalf("%s leader read %q", name, got)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("%s leader never received the transaction", name)
		}
	}
}

func TestSendReturnsOnFirstSuccess(t *testing.T) {
	old := sendAttemptTimeout
	sendAttemptTimeout = 3 * time.Second
	t.Cleanup(func() { sendAttemptTimeout = old })

	good, _, payloads := startTPUServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newTPURelay(t, ctx, blackhole(t), good)
	r.checkAndSetSlot(100)

	// the first leader never answers; the caller must not wait out its timeout
	start := time.Now()
	if err := r.SendRawTransaction(ctx, []byte("tx"), 1, 2); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("send took %s, want the time of the fast leader", took)
	}
	select {
	case <-payloads:
	case <-time.After(3 * time.Second):
		t.Fatal("responsive leader never received the transaction")
	}
}

func TestSendFailsWhenEveryLeaderFails(t *testing.T) {
	old := sendAttemptTimeout
	sendAttemptTimeout = 100 * time.Millisecond
	t.Cleanup(func() { sendAttemptTimeout = old })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newTPURelay(t, ctx, blackhole(t), blackhole(t))
	r.checkAndSetSlot(100)

	err := r.SendRawTransaction(ctx, []byte("tx"), 1, 2)
	if err == nil {
		t.Fatal("send succeeded with no reachable leader")
	}
	for _, id := range []byte{1, 2} {
		if !strings.Contains(err.Error(), pk(id).String()) {
			t.Errorf("error does not mention leader %d: %v", id, err)
		}
	}
}
