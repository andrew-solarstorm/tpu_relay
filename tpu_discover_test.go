package tpu_relay

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

// startTPUServer runs a loopback QUIC listener speaking the TPU ALPN. It counts
// accepted connections and delivers every uni-stream payload on the returned channel.
func startTPUServer(t *testing.T) (addr string, accepted *atomic.Int32, payloads chan []byte) {
	t.Helper()

	tlsConf, err := createTlsConfig()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := quic.ListenAddr("127.0.0.1:0", tlsConf, nil)
	if err != nil {
		t.Skipf("cannot listen on loopback UDP: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted = new(atomic.Int32)
	payloads = make(chan []byte, 16)
	go func() {
		for {
			conn, err := ln.Accept(context.Background())
			if err != nil {
				return
			}
			accepted.Add(1)
			go func() {
				for {
					st, err := conn.AcceptUniStream(context.Background())
					if err != nil {
						return
					}
					b, _ := io.ReadAll(st)
					payloads <- b
				}
			}()
		}
	}()

	return ln.Addr().String(), accepted, payloads
}

func TestConnectSharesInflightDial(t *testing.T) {
	addr, accepted, _ := startTPUServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc, err := NewTPUDiscover(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	leader := &LeaderAtSlot{Slot: 1, Info: &Leader{Pubkey: pk(1), TPUQUIC: addr}}

	const callers = 20
	conns := make([]*LeaderConn, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := svc.Connect(ctx, leader)
			if err != nil {
				t.Error(err)
			}
			conns[i] = c
		}()
	}
	wg.Wait()

	for _, c := range conns {
		if c != conns[0] {
			t.Fatal("concurrent callers got different connections")
		}
	}
	time.Sleep(100 * time.Millisecond) // let any stray extra dial reach the server
	if n := accepted.Load(); n != 1 {
		t.Fatalf("server accepted %d connections, want 1", n)
	}
}

func TestSendDeliversAndRedialsDeadConn(t *testing.T) {
	addr, accepted, payloads := startTPUServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc, err := NewTPUDiscover(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	leader := &LeaderAtSlot{Slot: 1, Info: &Leader{Pubkey: pk(1), TPUQUIC: addr}}

	if err := svc.Send(ctx, leader, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-payloads:
		if string(got) != "hello" {
			t.Fatalf("server read %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server never received the payload")
	}

	// kill the pooled connection; the next send must dial a fresh one
	old := svc.live(leader.Info.Pubkey)
	_ = old.Conn.CloseWithError(0, "test")
	if err := svc.Send(ctx, leader, []byte("again")); err != nil {
		t.Fatal(err)
	}
	if got := svc.live(leader.Info.Pubkey); got == nil || got == old {
		t.Fatal("expected a fresh connection after the old one died")
	}
	// Dial returns before the server's Accept loop has necessarily run
	deadline := time.Now().Add(2 * time.Second)
	for accepted.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := accepted.Load(); n != 2 {
		t.Fatalf("server accepted %d connections, want 2", n)
	}
}

func TestCloseClosesPooledConns(t *testing.T) {
	addr, _, _ := startTPUServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc, err := NewTPUDiscover(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}

	leader := &LeaderAtSlot{Slot: 1, Info: &Leader{Pubkey: pk(1), TPUQUIC: addr}}
	conn, err := svc.Connect(ctx, leader)
	if err != nil {
		t.Fatal(err)
	}

	svc.Close()
	if conn.Conn.Context().Err() == nil {
		t.Fatal("connection still open after Close")
	}
}
