package golink_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/golink"
	"github.com/floatdrop/golink/golinktest"
	"github.com/floatdrop/golink/internal/testpb"
)

// echo replies Pong{N+1} to Ping messages and calls, errors on N < 0, stops
// on N == 0 with "normal", and crashes with "boom" on N == -100.
func echo(p *golink.Process[*testpb.Ping]) error {
	for {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		if m.Down != nil {
			continue
		}
		switch {
		case m.Body.N == 0:
			return nil
		case m.Body.N == -100:
			return errors.New("boom")
		case m.Body.N == -200:
			panic("kaboom")
		case m.IsCall() && m.Body.N < 0:
			_ = p.Reply(m, nil, errors.New("negative: "+strconv.FormatInt(m.Body.N, 10)))
		case m.IsCall():
			_ = p.Reply(m, &testpb.Pong{N: m.Body.N + 1}, nil)
		default:
			_ = p.SendTo(m.From, &testpb.Pong{N: m.Body.N + 1})
		}
	}
}

// collector is an untyped process that forwards everything it receives to a channel.
func collector(t testing.TB, n *golink.Node) (golink.Addr[proto.Message], <-chan golink.Msg[proto.Message]) {
	t.Helper()
	ch := make(chan golink.Msg[proto.Message], 4096)
	addr, err := golink.Spawn[proto.Message](n, func(p *golink.Process[proto.Message]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			ch <- m
		}
	}, golink.WithLabel("collector"))
	if err != nil {
		t.Fatal(err)
	}
	return addr, ch
}

// watcher is a process that monitors whatever PIDs it is sent and forwards Downs.
func watcher(t testing.TB, n *golink.Node) (*golink.Process[proto.Message], <-chan golink.Msg[proto.Message]) {
	t.Helper()
	ch := make(chan golink.Msg[proto.Message], 64)
	ready := make(chan *golink.Process[proto.Message], 1)
	_, err := golink.Spawn[proto.Message](n, func(p *golink.Process[proto.Message]) error {
		ready <- p
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			ch <- m
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return <-ready, ch
}

func recv[M proto.Message](t testing.TB, ch <-chan golink.Msg[M]) golink.Msg[M] {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
		return golink.Msg[M]{}
	}
}

func ctx(t testing.TB) context.Context {
	c, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

type countingHooks struct {
	golink.NopHooks
	spawns, exits, sends, receives, deadLetters, linkUps, linkDowns atomic.Int64
	lastDead                                                        atomic.Pointer[string]
}

func (h *countingHooks) OnSpawn(golink.ProcessInfo)        { h.spawns.Add(1) }
func (h *countingHooks) OnExit(golink.ProcessInfo, string) { h.exits.Add(1) }
func (h *countingHooks) OnSend(_ golink.SendInfo, md golink.Metadata) (golink.Metadata, golink.Done) {
	h.sends.Add(1)
	return md, nil
}
func (h *countingHooks) OnReceive(_ golink.ReceiveInfo, md golink.Metadata) (golink.Metadata, golink.Done) {
	h.receives.Add(1)
	return md, nil
}
func (h *countingHooks) OnDeadLetter(_, _ golink.PID, _ proto.Message, reason string) {
	h.deadLetters.Add(1)
	h.lastDead.Store(&reason)
}
func (h *countingHooks) OnLinkUp(golink.NodeID)          { h.linkUps.Add(1) }
func (h *countingHooks) OnLinkDown(golink.NodeID, error) { h.linkDowns.Add(1) }

func TestLocalTypedSendAndCall(t *testing.T) {
	c := golinktest.New(t, "a")
	a := c.Node("a")
	e, _ := golink.Spawn(a, echo)
	// Sending from outside any process: the Pong reply has nowhere to go, so
	// it is a dead letter, not an error.
	if err := a.Send(e, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	// From inside a process the reply comes back to it.
	gotc := make(chan golink.Msg[proto.Message], 1)
	_, err := golink.Spawn[proto.Message](a, func(p *golink.Process[proto.Message]) error {
		if err := p.Send(e, &testpb.Ping{N: 41}); err != nil {
			return err
		}
		m, err := p.Receive()
		if err != nil {
			return err
		}
		gotc <- m
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	r, err := a.Call[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 9})
	if err != nil || r.N != 10 {
		t.Fatalf("call: %v %v", r, err)
	}
	got := recv(t, gotc)
	if pong, ok := got.Body.(*testpb.Pong); !ok || pong.N != 42 || got.From != e.PID() {
		t.Fatalf("got %+v", got)
	}
	if _, err := a.Call[*testpb.Ping](ctx(t), e, &testpb.Ping{N: 1}); !errors.Is(err, golink.ErrType) {
		t.Fatalf("reply typed wrongly should be ErrType, got %v", err)
	}
}

func TestRemoteByPIDAndName(t *testing.T) {
	c := golinktest.New(t, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	e, err := golink.Spawn(b, echo, golink.WithName("echo"))
	if err != nil {
		t.Fatal(err)
	}
	col, ch := collector(t, a)

	// Same API as local: the address points at another node.
	w, _ := watcher(t, a)
	if err := w.Send(e, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	_ = col
	_ = ch
	r, err := a.CallTo[*testpb.Pong](ctx(t), golink.Named[*testpb.Ping]("b", "echo"), &testpb.Ping{N: 9})
	if err != nil || r.N != 10 {
		t.Fatalf("call by name: %v %v", r, err)
	}
	// A typed call from inside a process, reply type explicit, request inferred.
	_, err = golink.Spawn[proto.Message](a, func(p *golink.Process[proto.Message]) error {
		r, err := p.Call[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 99})
		if err != nil || r.N != 100 {
			t.Errorf("process call: %v %v", r, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Errors from handlers cross the wire.
	_, err = a.CallTo[*testpb.Pong](ctx(t), e, &testpb.Ping{N: -1})
	if re, ok := errors.AsType[*golink.RemoteError](err); !ok || re.Msg != "negative: -1" {
		t.Fatalf("want remote error, got %v", err)
	}
	// Unknown process, by PID and by name.
	if _, err = a.CallTo[*testpb.Pong](ctx(t), golink.PID{Node: "b", Incarnation: e.PID().Incarnation, ID: 9999}, &testpb.Ping{}); !errors.Is(err, golink.ErrNoProc) {
		t.Fatalf("want ErrNoProc, got %v", err)
	}
	if _, err = a.CallTo[*testpb.Pong](ctx(t), golink.Named[*testpb.Ping]("b", "nope"), &testpb.Ping{}); !errors.Is(err, golink.ErrNoProc) {
		t.Fatalf("want ErrNoProc by name, got %v", err)
	}
	if peers := a.Peers(); len(peers) != 1 || peers[0] != "b" {
		t.Fatalf("peers %v", peers)
	}
}

func TestTypeMismatchIsDeadLetter(t *testing.T) {
	h := &countingHooks{}
	c := golinktest.NewWith(t, []golinktest.Option{golinktest.WithHooks(h)}, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	e, _ := golink.Spawn(b, echo, golink.WithName("echo"))

	// A remote sender addresses the process with the wrong type.
	wrong := golink.Named[*testpb.Pong]("b", "echo")
	if err := a.SendTo(wrong, &testpb.Pong{N: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CallTo[*testpb.Pong](ctx(t), wrong, &testpb.Pong{}); !errors.Is(err, golink.ErrType) {
		t.Fatalf("want ErrType, got %v", err)
	}
	// Locally too, through an untyped address.
	if err := b.SendTo(e.PID(), &testpb.Pong{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for h.deadLetters.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := h.deadLetters.Load(); got != 3 {
		t.Fatalf("dead letters = %d, want 3", got)
	}
	if r := h.lastDead.Load(); r == nil || *r != golink.ReasonType {
		t.Fatalf("reason %v", r)
	}
	if b.Info().DeadLetters != 3 {
		t.Fatalf("node info dead letters = %d", b.Info().DeadLetters)
	}
	// The process is unharmed.
	if r, err := a.CallTo[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); err != nil || r.N != 2 {
		t.Fatalf("%v %v", r, err)
	}
}

func TestRemoteOrdering(t *testing.T) {
	c := golinktest.New(t, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	col, ch := collector(t, b)
	const N = 20000
	_, _ = golink.Spawn[proto.Message](a, func(p *golink.Process[proto.Message]) error {
		for i := range N {
			if err := p.Send(col, proto.Message(&testpb.Ping{N: int64(i)})); err != nil {
				return err
			}
		}
		return nil
	})
	for i := range N {
		if m := recv(t, ch); m.Body.(*testpb.Ping).N != int64(i) {
			t.Fatalf("out of order: want %d got %v", i, m.Body)
		}
	}
}

func TestMonitorReasons(t *testing.T) {
	c := golinktest.New(t, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	w, ch := watcher(t, a)

	cases := []struct {
		name   string
		kill   func(e golink.Addr[*testpb.Ping])
		reason string
	}{
		{"normal", func(e golink.Addr[*testpb.Ping]) { _ = w.Send(e, &testpb.Ping{N: 0}) }, golink.ReasonNormal},
		{"error", func(e golink.Addr[*testpb.Ping]) { _ = w.Send(e, &testpb.Ping{N: -100}) }, "boom"},
		{"panic", func(e golink.Addr[*testpb.Ping]) { _ = w.Send(e, &testpb.Ping{N: -200}) }, "panic: kaboom"},
		{"exit", func(e golink.Addr[*testpb.Ping]) { _ = w.Exit(e, golink.ReasonKilled) }, golink.ReasonKilled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, _ := golink.Spawn(b, echo)
			ref := w.Monitor(e)
			// A call round-trips on the same link, so the monitor is installed first.
			if _, err := w.Call[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); err != nil {
				t.Fatal(err)
			}
			tc.kill(e)
			m := recv(t, ch)
			if m.Down == nil || m.Down.Ref != ref || m.Down.PID != e.PID() || m.Down.Reason != tc.reason {
				t.Fatalf("got %+v, want reason %q", m.Down, tc.reason)
			}
		})
	}
	t.Run("noproc", func(t *testing.T) {
		ref := w.Monitor(golink.PID{Node: "b", Incarnation: b.ID().Incarnation, ID: 424242})
		m := recv(t, ch)
		if m.Down == nil || m.Down.Ref != ref || m.Down.Reason != golink.ReasonNoProc {
			t.Fatalf("got %+v", m.Down)
		}
	})
	t.Run("stale incarnation", func(t *testing.T) {
		e, _ := golink.Spawn(b, echo)
		old := e.PID()
		old.Incarnation--
		w.Monitor(old)
		if m := recv(t, ch); m.Down == nil || m.Down.Reason != golink.ReasonNoProc {
			t.Fatalf("got %+v", m.Down)
		}
	})
	t.Run("by name", func(t *testing.T) {
		e, _ := golink.Spawn(b, echo, golink.WithName("named"))
		ref := w.Monitor(golink.Name{Node: "b", Name: "named"})
		if _, err := w.Call[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		_ = w.Send(e, &testpb.Ping{N: 0})
		if m := recv(t, ch); m.Down == nil || m.Down.Ref != ref || m.Down.PID != e.PID() || m.Down.Name != "named" {
			t.Fatalf("got %+v", m.Down)
		}
	})
	t.Run("by name, node lost", func(t *testing.T) {
		e, _ := golink.Spawn(b, echo, golink.WithName("lost"))
		ref := w.Monitor(golink.Name{Node: "b", Name: "lost"})
		if _, err := w.Call[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		a.Disconnect("b")
		// Which process held the name is unknown here: only the name is.
		if m := recv(t, ch); m.Down == nil || m.Down.Ref != ref || m.Down.Name != "lost" || m.Down.PID != (golink.PID{Node: "b"}) || m.Down.Reason != golink.ReasonNoConnection {
			t.Fatalf("got %+v", m.Down)
		}
	})
}

func TestDemonitor(t *testing.T) {
	c := golinktest.New(t, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	w, ch := watcher(t, a)
	e, _ := golink.Spawn(b, echo)
	ref := w.Monitor(e)
	w.Demonitor(ref)
	_ = w.Send(e, &testpb.Ping{N: 0})
	select {
	case m := <-ch:
		t.Fatalf("unexpected %+v", m)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestKillAndRestart(t *testing.T) {
	h := &countingHooks{}
	c := golinktest.NewWith(t, []golinktest.Option{golinktest.WithHooks(h)}, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	w, ch := watcher(t, a)

	silent, _ := golink.Spawn[proto.Message](b, func(p *golink.Process[proto.Message]) error {
		for {
			if _, err := p.Receive(); err != nil {
				return err
			}
		}
	}, golink.WithName("silent"))
	w.Monitor(silent)
	callErr := make(chan error, 1)
	go func() {
		_, err := a.CallTo[*testpb.Pong](t.Context(), silent, &testpb.Ping{})
		callErr <- err
	}()
	time.Sleep(100 * time.Millisecond)

	c.Kill("b")

	select {
	case err := <-callErr:
		if !errors.Is(err, golink.ErrNoConnection) {
			t.Fatalf("want ErrNoConnection, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pending call not failed on node down")
	}
	if m := recv(t, ch); m.Down == nil || m.Down.PID != silent.PID() || m.Down.Reason != golink.ReasonNoConnection {
		t.Fatalf("got %+v", m.Down)
	}
	if err := a.SendTo(silent, &testpb.Ping{}); !errors.Is(err, golink.ErrNoConnection) {
		t.Fatalf("send to dead node: %v", err)
	}

	b2 := c.Restart("b")
	if b2.ID().Incarnation == b.ID().Incarnation {
		t.Fatal("incarnation did not change")
	}
	e, _ := golink.Spawn(b2, echo, golink.WithName("echo"))
	if r, err := a.CallTo[*testpb.Pong](ctx(t), golink.Named[*testpb.Ping]("b", "echo"), &testpb.Ping{N: 5}); err != nil || r.N != 6 {
		t.Fatalf("after restart: %v %v", r, err)
	}
	// The old PID names a process of the old incarnation: noproc, never a
	// delivery to whatever now has that id.
	w.Monitor(silent)
	if m := recv(t, ch); m.Down == nil || m.Down.Reason != golink.ReasonNoProc {
		t.Fatalf("stale pid: %+v", m.Down)
	}
	_ = e
	if h.linkDowns.Load() == 0 {
		t.Fatal("OnLinkDown not called")
	}
}

func TestPartitionAndHeal(t *testing.T) {
	c := golinktest.New(t, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	w, ch := watcher(t, a)
	e, _ := golink.Spawn(b, echo, golink.WithName("echo"))
	w.Monitor(e)
	if _, err := w.Call[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	c.Partition("a", "b")
	if m := recv(t, ch); m.Down == nil || m.Down.Reason != golink.ReasonNoConnection {
		t.Fatalf("got %+v", m.Down)
	}
	if _, err := a.CallTo[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); !errors.Is(err, golink.ErrNoConnection) {
		t.Fatalf("partitioned call: %v", err)
	}
	c.Heal("a", "b")
	if r, err := a.CallTo[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); err != nil || r.N != 2 {
		t.Fatalf("after heal: %v %v", r, err)
	}
	// The process on b survived the partition; only the link was lost.
	if _, ok := b.Process(e.PID()); !ok {
		t.Fatal("process gone")
	}
	info := a.Info()
	if len(info.Links) == 0 || info.Links[0].Reconnects != 1 {
		t.Fatalf("links %+v", info.Links)
	}
}

func TestGracefulStopSendsShutdown(t *testing.T) {
	c := golinktest.New(t, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	w, ch := watcher(t, a)
	e, _ := golink.Spawn(b, echo)
	w.Monitor(e)
	if _, err := w.Call[*testpb.Pong](ctx(t), e, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	c.Stop("b")
	if m := recv(t, ch); m.Down == nil || m.Down.Reason != golink.ReasonShutdown {
		t.Fatalf("got %+v", m.Down)
	}
}

func TestLeftoverCallsFailFast(t *testing.T) {
	c := golinktest.New(t, "a")
	a := c.Node("a")
	block := make(chan struct{})
	e, _ := golink.Spawn[*testpb.Ping](a, func(p *golink.Process[*testpb.Ping]) error { <-block; return nil })
	errc := make(chan error, 1)
	go func() { _, err := a.CallTo[*testpb.Pong](t.Context(), e, &testpb.Ping{}); errc <- err }()
	time.Sleep(50 * time.Millisecond)
	close(block)
	select {
	case err := <-errc:
		if !errors.Is(err, golink.ErrNoProc) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("call hung")
	}
}

func TestRegistry(t *testing.T) {
	c := golinktest.New(t, "a")
	a := c.Node("a")
	e, _ := golink.Spawn(a, echo, golink.WithName("svc"))
	if _, err := golink.Spawn(a, echo, golink.WithName("svc")); !errors.Is(err, golink.ErrNameTaken) {
		t.Fatalf("got %v", err)
	}
	if got, ok := a.Whereis("svc"); !ok || got != e.PID() {
		t.Fatal("whereis")
	}
	w, ch := watcher(t, a)
	w.Monitor(e)
	_ = a.SendTo(golink.Name{Node: "a", Name: "svc"}, &testpb.Ping{N: 0})
	recv(t, ch)
	if _, ok := a.Whereis("svc"); ok {
		t.Fatal("name not released on exit")
	}
}

func TestInspectAndInfo(t *testing.T) {
	c := golinktest.New(t, "a")
	a := c.Node("a")
	type state struct{ handled int }
	s := &state{}
	release := make(chan struct{})
	e, _ := golink.Spawn[*testpb.Ping](a, func(p *golink.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			s.handled++
			if m.Body.N == 7 {
				<-release // simulate a long handler
			}
		}
	}, golink.WithName("insp"), golink.WithLabel("order"), golink.WithInspect(func() map[string]string {
		return map[string]string{"handled": strconv.Itoa(s.handled)}
	}))
	for range 3 {
		_ = a.SendTo(e, &testpb.Ping{N: 1})
	}
	time.Sleep(20 * time.Millisecond)
	got, err := a.Inspect(ctx(t), e.PID())
	if err != nil || got["handled"] != "3" {
		t.Fatalf("inspect: %v %v", got, err)
	}
	info, _ := a.Process(e.PID())
	if info.Label != "order" || info.Names[0] != "insp" || info.Received != 3 || info.State != golink.StateIdle ||
		info.LastMessage != "golink.test.v1.Ping" || info.Type != "*testpb.Ping" {
		t.Fatalf("info %+v", info)
	}
	// Busy process: inspect times out with a reason, and the mailbox shows the backlog.
	_ = a.SendTo(e, &testpb.Ping{N: 7})
	_ = a.SendTo(e, &testpb.Ping{N: 1})
	time.Sleep(20 * time.Millisecond)
	short, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := a.Inspect(short, e.PID()); err == nil || !strings.Contains(err.Error(), "busy") {
		t.Fatalf("want busy error, got %v", err)
	}
	info, _ = a.Process(e.PID())
	if info.State != golink.StateRunning || info.Mailbox.Depth != 1 || info.Mailbox.OldestAge <= 0 {
		t.Fatalf("busy info %+v", info)
	}
	close(release)
	ni := a.Info()
	if ni.ID.Name != "a" || ni.Processes != 1 || ni.Spawned != 1 {
		t.Fatalf("node info %+v", ni)
	}
	if all := a.Processes(); len(all) != 1 || all[0].PID != e.PID() {
		t.Fatalf("processes %+v", all)
	}
}

func TestMetadataPropagates(t *testing.T) {
	c := golinktest.New(t, "a", "b")
	a, b := c.Node("a"), c.Node("b")
	col, ch := collector(t, b)
	if err := a.SendContext(golink.WithMetadata(t.Context(), golink.Metadata{"trace": "abc"}), col, proto.Message(&testpb.Ping{})); err != nil {
		t.Fatal(err)
	}
	if m := recv(t, ch); m.Metadata["trace"] != "abc" {
		t.Fatalf("metadata %v", m.Metadata)
	}
}

// ---------- benchmarks ----------

func BenchmarkLocalCall(b *testing.B) {
	c := golinktest.New(b, "a")
	a := c.Node("a")
	e, _ := golink.Spawn(a, echo)
	for b.Loop() {
		if _, err := a.Call[*testpb.Pong](b.Context(), e, &testpb.Ping{N: 1}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRemoteCall(b *testing.B) {
	c := golinktest.New(b, "a", "b")
	a := c.Node("a")
	e, _ := golink.Spawn(c.Node("b"), echo)
	_, _ = a.Call[*testpb.Pong](b.Context(), e, &testpb.Ping{N: 1}) // establish the link
	for b.Loop() {
		if _, err := a.Call[*testpb.Pong](b.Context(), e, &testpb.Ping{N: 1}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRemoteCallParallel(b *testing.B) {
	c := golinktest.New(b, "a", "b")
	a := c.Node("a")
	e, _ := golink.Spawn(c.Node("b"), echo)
	_, _ = a.Call[*testpb.Pong](b.Context(), e, &testpb.Ping{N: 1})
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := a.Call[*testpb.Pong](b.Context(), e, &testpb.Ping{N: 1}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// benchSend measures sends until every one is received, so the number is
// end-to-end throughput, not the cost of enqueueing. It uses b.N rather than
// b.Loop on purpose: b.Loop stops the timer when the loop ends, before the
// wait for the receiver, which would time only the enqueue.
func benchSend(b *testing.B, from, to *golink.Node) {
	done := make(chan struct{})
	total := b.N
	addr, err := golink.Spawn[*testpb.Ping](to, func(p *golink.Process[*testpb.Ping]) error {
		for range total + 1 {
			if _, err := p.Receive(); err != nil {
				return err
			}
		}
		close(done)
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	msg := &testpb.Ping{}
	_ = from.Send(addr, msg) // establish the link before timing
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := from.Send(addr, msg); err != nil {
			b.Fatal(err)
		}
	}
	<-done
}

func BenchmarkLocalSend(b *testing.B) {
	c := golinktest.New(b, "a")
	benchSend(b, c.Node("a"), c.Node("a"))
}

func BenchmarkRemoteSend(b *testing.B) {
	c := golinktest.New(b, "a", "b")
	benchSend(b, c.Node("a"), c.Node("b"))
}

func TestBusyMeasuresTheCurrentMessage(t *testing.T) {
	c := golinktest.New(t, "a")
	a := c.Node("a")
	release := make(chan struct{})
	defer close(release)
	p, _ := golink.Spawn[*testpb.Ping](a, func(p *golink.Process[*testpb.Ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.Body.GetN() == 1 {
				<-release
			}
		}
	})
	time.Sleep(300 * time.Millisecond) // old, but idle
	_ = a.Send(p, &testpb.Ping{N: 1})
	time.Sleep(20 * time.Millisecond)
	short, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	_, err := a.Inspect(short, p.PID())
	if d := busyFor(t, err); d > 200*time.Millisecond {
		t.Fatalf("busy must count from the message, not the start: %v", err)
	}
	// A process that never took a message counts from its start.
	stuck, _ := golink.Spawn[*testpb.Ping](a, func(*golink.Process[*testpb.Ping]) error { <-release; return nil })
	time.Sleep(50 * time.Millisecond)
	short2, cancel2 := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel2()
	_, err = a.Inspect(short2, stuck.PID())
	if d := busyFor(t, err); d < 50*time.Millisecond {
		t.Fatalf("busy since start: %v", err)
	}
	// Untyped processes are named proto.Message, not by the alias's target.
	u, _ := golink.Spawn[proto.Message](a, func(p *golink.Process[proto.Message]) error { _, err := p.Receive(); return err })
	if info, _ := a.Process(u.PID()); info.Type != "proto.Message" || info.Label != "proto.Message" {
		t.Fatalf("%+v", info)
	}
}

func busyFor(t *testing.T, err error) time.Duration {
	t.Helper()
	if err == nil {
		t.Fatal("inspect of a busy process answered")
	}
	_, rest, ok := strings.Cut(err.Error(), "busy for ")
	d, perr := time.ParseDuration(strings.SplitN(rest, ":", 2)[0])
	if !ok || perr != nil {
		t.Fatalf("no duration in %v", err)
	}
	return d
}
