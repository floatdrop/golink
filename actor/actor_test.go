package actor_test

import (
	"errors"
	"fmt"
	"log/slog"
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
		go func() { _ = m.Reply(&testpb.Pong{N: 42}, nil) }()
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
	_, err := n.Spawn[proto.Message](func(p *grpcproc.Process[proto.Message]) error {
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
	addr, err := n.Spawn(actor.Run[*testpb.Ping](h))
	if err != nil {
		t.Fatal(err)
	}
	downs := watch(t, n, addr)
	for range 3 {
		_ = n.Send(t.Context(), addr, &testpb.Ping{N: 1})
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
		{"message error", func(n *grpcproc.Node, a grpcproc.Addr[*testpb.Ping]) { _ = n.Send(t.Context(), a, &testpb.Ping{N: -1}) }, "bad message", "init; terminate: bad message"},
		{"stop", func(n *grpcproc.Node, a grpcproc.Addr[*testpb.Ping]) { _ = n.Send(t.Context(), a, &testpb.Ping{N: -2}) }, grpcproc.ReasonNormal, "init; terminate"},
		{"panic", func(n *grpcproc.Node, a grpcproc.Addr[*testpb.Ping]) { _ = n.Send(t.Context(), a, &testpb.Ping{N: -3}) }, "panic: kaboom", "init; terminate: panic: kaboom"},
		{"exit", func(n *grpcproc.Node, a grpcproc.Addr[*testpb.Ping]) { _ = n.Exit(t.Context(), a, "bye") }, "bye", "init; terminate: grpcproc: exit: bye"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := grpcproctest.New(t, "a")
			n := c.Node("a")
			h := &counter{}
			addr, _ := n.Spawn(actor.Run[*testpb.Ping](h))
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
	addr, _ := n.Spawn(actor.Run[*testpb.Ping](h))
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
	target, _ := n.Spawn[*testpb.Ping](func(p *P) error { _, err := p.Receive(); return err })
	_, _ = n.Spawn(actor.Run[*testpb.Ping](&monitoring{target: target, counter: h}))
	time.Sleep(10 * time.Millisecond)
	_ = n.Exit(t.Context(), target, "gone")
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(h.Log(), "down gone") {
		if time.Now().After(deadline) {
			t.Fatalf("log: %s", h.Log())
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Without a CallHandler, calls are answered with an error.
	seen := make(chan int64, 4)
	pa, _ := n.Spawn(actor.Run[*testpb.Ping](plain{seen: seen}))
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
	target, _ := n.Spawn[*testpb.Ping](func(p *P) error { _, err := p.Receive(); return err })
	pa, _ := n.Spawn(actor.Run[*testpb.Ping](&monitoringPlain{plain: plain{seen: seen}, target: target}))
	_ = n.Exit(t.Context(), target, "gone")
	time.Sleep(20 * time.Millisecond)
	_ = n.Send(t.Context(), pa, &testpb.Ping{N: 9})
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

// quoter only answers calls.
type quoter struct{ actor.CallsOnly[*testpb.Ping] }

func (quoter) HandleCall(_ *P, m M) (proto.Message, error) {
	return &testpb.Pong{N: m.Body.GetN() + 1}, nil
}

// logBuffer is an io.Writer safe for the node's logger.
type logBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func TestCallsOnly(t *testing.T) {
	var logs logBuffer
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithLogger(slog.New(slog.NewTextHandler(&logs, nil)))}, "a")
	n := c.Node("a")
	addr, err := n.Spawn(actor.Run(quoter{}))
	if err != nil {
		t.Fatal(err)
	}
	// A plain send is dropped with a warning; the actor carries on.
	if err := n.Send(t.Context(), addr, &testpb.Ping{N: 1}); err != nil {
		t.Fatal(err)
	}
	r, err := n.Call[*testpb.Pong](t.Context(), addr, &testpb.Ping{N: 2})
	if err != nil || r.GetN() != 3 {
		t.Fatalf("%v %v", r, err)
	}
	if out := logs.String(); !strings.Contains(out, "dropped a message sent without a call") || !strings.Contains(out, "grpcproc.test.v1.Ping") {
		t.Fatalf("log:\n%s", out)
	}
}

// quoterByValue has a pointer-receiver HandleCall: spawned by value, it
// would answer nothing.
type quoterByValue struct{ actor.CallsOnly[*testpb.Ping] }

func (*quoterByValue) HandleCall(*P, M) (proto.Message, error) { return &testpb.Pong{}, nil }

func TestCallsOnlyWithoutHandleCallPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), "quoterByValue") {
			t.Fatalf("recovered %v", r)
		}
	}()
	_ = actor.Run(quoterByValue{})
}
