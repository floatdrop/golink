package actor_test

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

type P = grpcproc.Process[*testpb.Ping]
type M = grpcproc.Msg[*testpb.Ping]

// counter implements every optional interface and records what happens.
type counter struct {
	failInit bool
	count    int64

	mu  sync.Mutex
	log []string
}

func (c *counter) record(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log = append(c.log, s)
}

func (c *counter) Log() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.log, "; ")
}

func (c *counter) Init(*P) error {
	if c.failInit {
		return errors.New("init failed")
	}
	c.record("init")
	return nil
}

func (c *counter) HandleMessage(_ *P, m M) error {
	switch m.Body.GetN() {
	case -1:
		return errors.New("bad message")
	case -2:
		return actor.ErrStop
	case -3:
		panic("kaboom")
	}
	c.count++
	return nil
}

func (c *counter) HandleCall(p *P, m M) (proto.Message, error) {
	switch m.Body.GetN() {
	case -1:
		return nil, errors.New("bad call")
	case -2:
		return &testpb.Pong{N: c.count}, actor.ErrStop
	case -4:
		go func() { _ = p.Reply(m, &testpb.Pong{N: 42}, nil) }()
		return nil, actor.ErrNoReply
	}
	return &testpb.Pong{N: c.count}, nil
}

func (c *counter) HandleDown(_ *P, d grpcproc.Down) error {
	c.record("down " + d.Reason)
	return nil
}

func (c *counter) Terminate(_ *P, err error) {
	if err == nil {
		c.record("terminate")
		return
	}
	c.record("terminate: " + err.Error())
}

// plain handles messages only.
type plain struct{ seen chan int64 }

func (p plain) HandleMessage(_ *P, m M) error {
	p.seen <- m.Body.GetN()
	return nil
}

func watch(t *testing.T, n *grpcproc.Node, target grpcproc.Target) <-chan grpcproc.Down {
	t.Helper()
	ch := make(chan grpcproc.Down, 16)
	ready := make(chan struct{})
	_, err := grpcproc.Spawn[proto.Message](n, func(p *grpcproc.Process[proto.Message]) error {
		p.Monitor(target)
		close(ready)
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.Down != nil {
				ch <- *m.Down
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	<-ready
	return ch
}

func down(t *testing.T, ch <-chan grpcproc.Down) grpcproc.Down {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(5 * time.Second):
		t.Fatal("no Down")
		return grpcproc.Down{}
	}
}

func TestRunLifecycle(t *testing.T) {
	c := grpcproctest.New(t, "a")
	n := c.Node("a")
	h := &counter{}
	addr, err := actor.Spawn[*testpb.Ping](n, h)
	if err != nil {
		t.Fatal(err)
	}
	downs := watch(t, n, addr)
	for range 3 {
		_ = n.Send(addr, &testpb.Ping{N: 1})
	}
	if r, err := n.Call[*testpb.Pong](t.Context(), addr, &testpb.Ping{}); err != nil || r.GetN() != 3 {
		t.Fatalf("call: %v %v", r, err)
	}
	// An error from HandleCall goes to the caller; the actor carries on.
	if _, err := n.Call[*testpb.Pong](t.Context(), addr, &testpb.Ping{N: -1}); err == nil || err.Error() != "bad call" {
		t.Fatalf("call error: %v", err)
	}
	// A deferred reply, from another goroutine.
	if r, err := n.Call[*testpb.Pong](t.Context(), addr, &testpb.Ping{N: -4}); err != nil || r.GetN() != 42 {
		t.Fatalf("deferred: %v %v", r, err)
	}
	// ErrStop from a call replies first, then stops normally.
	if r, err := n.Call[*testpb.Pong](t.Context(), addr, &testpb.Ping{N: -2}); err != nil || r.GetN() != 3 {
		t.Fatalf("stop call: %v %v", r, err)
	}
	if d := down(t, downs); d.Reason != grpcproc.ReasonNormal {
		t.Fatalf("%+v", d)
	}
	if got := h.Log(); got != "init; terminate" {
		t.Fatalf("log: %s", got)
	}
}

func TestRunExits(t *testing.T) {
	cases := []struct {
		name   string
		act    func(n *grpcproc.Node, a grpcproc.Addr[*testpb.Ping])
		reason string
		log    string
	}{
		{"message error", func(n *grpcproc.Node, a grpcproc.Addr[*testpb.Ping]) { _ = n.Send(a, &testpb.Ping{N: -1}) }, "bad message", "init; terminate: bad message"},
		{"stop", func(n *grpcproc.Node, a grpcproc.Addr[*testpb.Ping]) { _ = n.Send(a, &testpb.Ping{N: -2}) }, grpcproc.ReasonNormal, "init; terminate"},
		{"panic", func(n *grpcproc.Node, a grpcproc.Addr[*testpb.Ping]) { _ = n.Send(a, &testpb.Ping{N: -3}) }, "panic: kaboom", "init; terminate: panic: kaboom"},
		{"exit", func(n *grpcproc.Node, a grpcproc.Addr[*testpb.Ping]) { _ = n.Exit(a, "bye") }, "bye", "init; terminate: grpcproc: exit: bye"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := grpcproctest.New(t, "a")
			n := c.Node("a")
			h := &counter{}
			addr, _ := actor.Spawn[*testpb.Ping](n, h)
			downs := watch(t, n, addr)
			tc.act(n, addr)
			if d := down(t, downs); d.Reason != tc.reason {
				t.Fatalf("reason %q", d.Reason)
			}
			if got := h.Log(); got != tc.log {
				t.Fatalf("log: %s", got)
			}
		})
	}
}

func TestInitFailureSkipsTerminate(t *testing.T) {
	c := grpcproctest.New(t, "a")
	n := c.Node("a")
	h := &counter{failInit: true}
	addr, _ := actor.Spawn[*testpb.Ping](n, h)
	downs := watch(t, n, addr)
	if d := down(t, downs); d.Reason != "init failed" && d.Reason != grpcproc.ReasonNoProc {
		t.Fatalf("%+v", d)
	}
	time.Sleep(10 * time.Millisecond)
	if got := h.Log(); got != "" {
		t.Fatalf("log: %s", got)
	}
}

func TestDownsAndOptionalInterfaces(t *testing.T) {
	c := grpcproctest.New(t, "a")
	n := c.Node("a")
	// With a DownHandler, Downs reach it.
	h := &counter{}
	target, _ := grpcproc.Spawn[*testpb.Ping](n, func(p *P) error { _, err := p.Receive(); return err })
	_, _ = actor.Spawn[*testpb.Ping](n, &monitoring{target: target, counter: h})
	time.Sleep(10 * time.Millisecond)
	_ = n.Exit(target, "gone")
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(h.Log(), "down gone") {
		if time.Now().After(deadline) {
			t.Fatalf("log: %s", h.Log())
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Without a CallHandler, calls are answered with an error.
	seen := make(chan int64, 4)
	pa, _ := actor.Spawn[*testpb.Ping](n, plain{seen: seen})
	if _, err := n.Call[*testpb.Pong](t.Context(), pa, &testpb.Ping{}); err == nil || !strings.Contains(err.Error(), "does not handle calls") {
		t.Fatalf("got %v", err)
	}
}

// monitoring monitors target in Init and hands Downs to counter.
type monitoring struct {
	target grpcproc.Addr[*testpb.Ping]
	*counter
}

func (m *monitoring) Init(p *P) error {
	p.Monitor(m.target)
	return nil
}

func TestPlainIgnoresDowns(t *testing.T) {
	c := grpcproctest.New(t, "a")
	n := c.Node("a")
	seen := make(chan int64, 4)
	target, _ := grpcproc.Spawn[*testpb.Ping](n, func(p *P) error { _, err := p.Receive(); return err })
	pa, _ := actor.Spawn[*testpb.Ping](n, &monitoringPlain{plain: plain{seen: seen}, target: target})
	_ = n.Exit(target, "gone")
	time.Sleep(20 * time.Millisecond)
	_ = n.Send(pa, &testpb.Ping{N: 9})
	if got := <-seen; got != 9 {
		t.Fatal(got) // the Down was skipped, the process is alive
	}
}

type monitoringPlain struct {
	plain
	target grpcproc.Addr[*testpb.Ping]
}

func (m *monitoringPlain) Init(p *P) error {
	p.Monitor(m.target)
	return nil
}
