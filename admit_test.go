package grpcproc_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"testing/synctest"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// admitted is a cluster of a and b in which b admits a's link with pol.
func admitted(t *testing.T, pol grpcproc.Policy) *grpcproctest.Cluster {
	t.Helper()
	return grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithConfig(func(name string, cfg *grpcproc.Config) {
		if name == "b" {
			cfg.Admit = func(_ context.Context, peer grpcproc.NodeID) (grpcproc.Policy, error) {
				if peer.Name == "a" {
					return pol, nil
				}
				return nil, nil
			}
		}
	})}, "a", "b")
}

// deadLetter waits for the next dead letter on events and checks its reason.
func deadLetter(t *testing.T, events <-chan grpcproc.Event, reason string) {
	t.Helper()
	if ev := nextEvent(t, events, grpcproc.EventDeadLetter); ev.Reason != reason {
		t.Fatalf("dead letter %q, want %q", ev.Reason, reason)
	}
}

// A peer admitted with Export reaches the exported names, by name or PID,
// and nothing else: what it may not reach does not exist for it.
func TestExport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := admitted(t, grpcproc.Export("ledger"))
		a, b := c.Node("a"), c.Node("b")
		ledger := spawnEcho(t, b, grpcproc.WithName("ledger"))
		spawnEcho(t, b, grpcproc.WithName("secret"))
		anon := spawnEcho(t, b)
		events := b.Subscribe(ctx(t), 64)
		call := func(to grpcproc.Addr[*testpb.Ping]) error {
			_, err := to.Call[*testpb.Pong](ctx(t), a, &testpb.Ping{N: 1})
			return err
		}

		if err := call(grpcproc.Named[*testpb.Ping]("b", "ledger")); err != nil {
			t.Fatalf("ledger by name: %v", err)
		}
		if err := call(grpcproc.AddrOf[*testpb.Ping](ledger.PID())); err != nil {
			t.Fatalf("ledger by PID: %v", err)
		}
		if err := call(grpcproc.Named[*testpb.Ping]("b", "secret")); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("secret: %v", err)
		}
		deadLetter(t, events, grpcproc.ReasonDenied)
		if err := call(grpcproc.AddrOf[*testpb.Ping](anon.PID())); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("a process without a name: %v", err)
		}
		deadLetter(t, events, grpcproc.ReasonDenied)
		gone := grpcproc.PID{Node: "b", Incarnation: ledger.PID().Incarnation, ID: 1 << 40}
		if err := call(grpcproc.AddrOf[*testpb.Ping](gone)); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Fatalf("a process that does not exist: %v", err)
		}
		deadLetter(t, events, grpcproc.ReasonNoProc)

		if err := a.SendTo(ctx(t), grpcproc.Name{Node: "b", Name: "secret"}, &testpb.Ping{N: 1}); err != nil {
			t.Fatal(err)
		}
		deadLetter(t, events, grpcproc.ReasonDenied)

		// An exit is refused even of an exported process: the call after it,
		// on the same link, finds the ledger running.
		if err := a.Exit(ctx(t), ledger.PID(), "stop"); err != nil {
			t.Fatal(err)
		}
		deadLetter(t, events, grpcproc.ReasonDenied)
		if err := call(ledger); err != nil {
			t.Fatalf("ledger after an exit: %v", err)
		}

		p, downs := watcher(t, a)
		p.Monitor(grpcproc.Name{Node: "b", Name: "secret"})
		if m := recv(t, downs); m.Down == nil || m.Down.Reason != grpcproc.ReasonNoProc {
			t.Fatalf("monitor of secret: %+v", m)
		}
		deadLetter(t, events, grpcproc.ReasonDenied)
		ref := p.Monitor(ledger.PID())
		waitWatchers(t, b, ledger.PID(), 1)
		p.Demonitor(ref)
		waitWatchers(t, b, ledger.PID(), 0)
	})
}

// Replies and Downs on a link a Policy judges always pass: they answer what
// the node that admitted the link asked.
func TestPolicyLetsAnswersThrough(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := admitted(t, func(grpcproc.Op, string) bool { return false })
		a, b := c.Node("a"), c.Node("b")
		target := spawnEcho(t, a)
		if r, err := target.Call[*testpb.Pong](ctx(t), b, &testpb.Ping{N: 1}); err != nil || r.N != 2 {
			t.Fatalf("call to a: %v, %v", r, err)
		}
		p, downs := watcher(t, b)
		p.Monitor(target.PID())
		waitWatchers(t, a, target.PID(), 1)
		if err := target.Send(ctx(t), a, &testpb.Ping{N: 0}); err != nil {
			t.Fatal(err)
		}
		if m := recv(t, downs); m.Down == nil || m.Down.Reason != grpcproc.ReasonNormal {
			t.Fatalf("down: %+v", m)
		}
	})
}

// A Policy sees each kind of request, and the name of the process it is for.
func TestPolicySeesOps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		seen := make(chan string, 8)
		c := admitted(t, func(op grpcproc.Op, name string) bool {
			seen <- op.String() + " " + name
			return true
		})
		a, b := c.Node("a"), c.Node("b")
		addr, _ := collector(t, b)
		if _, err := b.Spawn(func(p *grpcproc.Process[proto.Message]) error {
			<-p.Context().Done()
			return nil
		}, grpcproc.WithName("x")); err != nil {
			t.Fatal(err)
		}
		x := grpcproc.Name{Node: "b", Name: "x"}
		if err := a.SendTo(ctx(t), addr.PID(), &testpb.Ping{}); err != nil {
			t.Fatal(err)
		}
		if got := within(t, seen, "send"); got != "send " {
			t.Fatalf("got %q", got)
		}
		go func() {
			_, _ = grpcproc.AddrOf[proto.Message](x).Call[*testpb.Pong](context.Background(), a, &testpb.Ping{})
		}()
		if got := within(t, seen, "call"); got != "call x" {
			t.Fatalf("got %q", got)
		}
		p, _ := watcher(t, a)
		p.Monitor(x)
		if got := within(t, seen, "monitor"); got != "monitor x" {
			t.Fatalf("got %q", got)
		}
		if err := a.Exit(ctx(t), x, "stop"); err != nil {
			t.Fatal(err)
		}
		if got := within(t, seen, "exit"); got != "exit x" {
			t.Fatalf("got %q", got)
		}
	})
}

func TestOpString(t *testing.T) {
	for op, want := range map[grpcproc.Op]string{
		grpcproc.OpSend: "send", grpcproc.OpCall: "call", grpcproc.OpMonitor: "monitor",
		grpcproc.OpExit: "exit", grpcproc.Op(9): "op(9)",
	} {
		if got := op.String(); got != want {
			t.Errorf("%d: %q, want %q", op, got, want)
		}
	}
}

// DialOptionsFor adds options for one peer, after DialOptions, so they win:
// here a dialer that fails, for b only, for the node's link and for Dial.
func TestDialOptionsFor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithConfig(func(name string, cfg *grpcproc.Config) {
			if name != "a" {
				return
			}
			cfg.DialOptionsFor = func(peer string) []grpc.DialOption {
				if peer != "b" {
					return nil
				}
				return []grpc.DialOption{grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
					return nil, errors.New("b's own dialer")
				})}
			}
		})}, "a", "b", "c")
		a := c.Node("a")
		if err := a.SendTo(ctx(t), grpcproc.Name{Node: "b", Name: "x"}, &testpb.Ping{}); err == nil || !strings.Contains(err.Error(), "b's own dialer") {
			t.Fatalf("to b: %v", err)
		}
		if err := a.SendTo(ctx(t), grpcproc.Name{Node: "c", Name: "x"}, &testpb.Ping{}); err != nil {
			t.Fatalf("to c: %v", err)
		}
		cc, err := a.Dial(ctx(t), "b")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cc.Close() }()
		if _, err := grpcprocv1.NewNodeClient(cc).Link(ctx(t)); err == nil || !strings.Contains(err.Error(), "b's own dialer") {
			t.Fatalf("Dial to b: %v", err)
		}
	})
}
