package grpcproc_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// dialsHeld is a cluster in which a's dials to b wait until release is
// called (or the dial's deadline passes), after entered is closed.
func dialsHeld(t *testing.T) (c *grpcproctest.Cluster, entered <-chan struct{}, release func()) {
	t.Helper()
	in, out := make(chan struct{}), make(chan struct{})
	var once, released sync.Once
	c = grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithConfig(func(name string, cfg *grpcproc.Config) {
		if name != "a" {
			return
		}
		cfg.DialTimeout = 3 * time.Second
		cfg.Resolver = grpcproc.ResolverFunc(func(ctx context.Context, node string) (string, error) {
			if node == "b" {
				once.Do(func() { close(in) })
				select {
				case <-out:
				case <-ctx.Done():
					return "", ctx.Err()
				}
			}
			return "passthrough:///" + node, nil
		})
	})}, "a", "b")
	release = func() { released.Do(func() { close(out) }) }
	t.Cleanup(release) // before the cluster stops, which waits for the dial
	return c, in, release
}

// An answer dispatch makes itself, "no such process" to a peer this node
// has no link to yet, does not hold the peer's link while it dials:
// Disconnect, which closes that link, does not wait out the dial.
func TestDispatchAnswersDoNotWaitForADial(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, entered, _ := dialsHeld(t)
		a, b := c.Node("a"), c.Node("b")
		called := make(chan error, 1)
		go func() {
			_, err := grpcproc.Named[*testpb.Ping]("a", "nobody").Call[*testpb.Ping](context.Background(), b, &testpb.Ping{})
			called <- err
		}()
		within(t, entered, "dial")
		start := time.Now()
		a.Disconnect("b")
		if waited := time.Since(start); waited > time.Second {
			t.Fatalf("Disconnect waited %v for a dial", waited)
		}
		if err := within(t, called, "answer"); !errors.Is(err, grpcproc.ErrNoConnection) {
			t.Fatalf("got %v", err)
		}
	})
}

// Answers that wait for a dial go out in the order they were made, once the
// link is up.
func TestDispatchAnswersKeepTheirOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, entered, release := dialsHeld(t)
		const n = 50
		downs := make(chan string, n)
		if _, err := c.Node("b").Spawn(func(p *grpcproc.Process[proto.Message]) error {
			for i := range n {
				p.Monitor(grpcproc.Name{Node: "a", Name: fmt.Sprint("x", i)})
			}
			for {
				m, err := p.Receive()
				if err != nil {
					return err
				}
				downs <- m.Down.Name + " " + m.Down.Reason
			}
		}); err != nil {
			t.Fatal(err)
		}
		within(t, entered, "dial")
		release()
		for i := range n {
			if got, want := within(t, downs, "Down"), fmt.Sprint("x", i, " noproc"); got != want {
				t.Fatalf("Down %d: got %q, want %q", i, got, want)
			}
		}
	})
}
