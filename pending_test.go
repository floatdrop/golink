package grpcproc

import (
	"context"
	"errors"
	"testing"
	"time"

	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
	"google.golang.org/protobuf/proto"
)

type panickingHooks struct{ NopHooks }

func (panickingHooks) OnDeadLetter(PID, PID, proto.Message, string) { panic("hook") }

// Every way a call can end leaves nothing in pending: an answer takes its
// entry, and a call that ends unanswered, however, removes its own.
func TestCallsLeaveNothingPending(t *testing.T) {
	n, err := NewNode(Config{Name: "a", Resolver: StaticResolver{}, Hooks: panickingHooks{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	type ping = grpcprocv1.Hello
	echo, _ := n.Spawn(func(p *Process[*ping]) error {
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.Body.GetNode() != "silent" {
				_ = p.Reply(m, m.Body, nil)
			}
		}
	})
	ctx := context.Background()
	if _, err := n.Call[*ping](ctx, echo, &ping{}); err != nil { // answered
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if _, err := n.Call[*ping](short, echo, &ping{Node: "silent"}); !errors.Is(err, context.DeadlineExceeded) { // ctx
		t.Fatal(err)
	}
	if _, err := n.Call[*ping](ctx, Named[*ping]("", "x"), &ping{}); err == nil { // route: no node
		t.Fatal("routed to no node")
	}
	func() { // a hook panics on the way: no such process, a dead letter
		defer func() { _ = recover() }()
		_, _ = n.Call[*ping](ctx, Named[*ping]("a", "nobody"), &ping{})
	}()
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.pending) != 0 {
		t.Fatalf("%d calls left pending", len(n.pending))
	}
}
