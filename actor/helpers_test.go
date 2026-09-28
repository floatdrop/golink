package actor_test

import (
	"errors"
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

// worker exits normally on N == 0, fails on N < 0, and otherwise waits.
func worker(p *P) error {
	for {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		switch n := m.Body.GetN(); {
		case n == 0:
			return nil
		case n < 0:
			return errors.New("crash")
		}
	}
}

// watch monitors target from a process of its own and hands over its Downs.
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

// within takes the next value from ch, and fails the test if none comes.
func within[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
		var zero T
		return zero
	}
}

// until waits for cond to hold, and fails the test with what if it never does.
func until(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(2 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("never: %s", what)
		}
	}
}

// pidOf waits until name is registered, and returns its PID.
func pidOf(t *testing.T, n *grpcproc.Node, name string) grpcproc.PID {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if pid, ok := n.Whereis(name); ok {
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never registered", name)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// restarted waits until name is registered to a PID other than old.
func restarted(t *testing.T, n *grpcproc.Node, name string, old grpcproc.PID) grpcproc.PID {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if pid, ok := n.Whereis(name); ok && pid != old {
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was not restarted", name)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// send sends Ping{N: v} to the process registered as name on n.
func send(t *testing.T, n *grpcproc.Node, name string, v int64) {
	_ = grpcproc.Named[*testpb.Ping](n.Name(), name).Send(t.Context(), n, &testpb.Ping{N: v})
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

// logged starts node a, which logs to the logBuffer it returns.
func logged(t *testing.T) (*grpcproc.Node, *logBuffer) {
	t.Helper()
	logs := &logBuffer{}
	c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithLogger(slog.New(slog.NewTextHandler(logs, nil)))}, "a")
	return c.Node("a"), logs
}
