package grpcproctest_test

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/internal/testpb"
)

// Partition and Stop may run at once, over links in use: the race detector
// watches the state they share, and the watcher still hears the process go.
func TestPartitionWhileANodeStops(t *testing.T) {
	for i := range 5 {
		t.Run("", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := grpcproctest.New(t, "a", "b")
				a, b := c.Node("a"), c.Node("b")
				echo, err := b.Spawn(func(p *grpcproc.Process[*testpb.Ping]) error {
					for {
						m, err := p.Receive()
						if err != nil {
							return err
						}
						_ = m.Reply(m.Body, nil)
					}
				})
				if err != nil {
					t.Fatal(err)
				}
				downs := make(chan grpcproc.Down, 1)
				called := make(chan error, 1)
				if _, err := a.Spawn(func(p *grpcproc.Process[proto.Message]) error {
					p.Monitor(echo)
					_, err := echo.Call[*testpb.Ping](context.Background(), p, &testpb.Ping{N: int64(i)})
					called <- err
					m, err := p.Receive()
					if err != nil {
						return err
					}
					downs <- *m.Down
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := <-called; err != nil { // the links are up both ways
					t.Fatal(err)
				}
				stopped := make(chan struct{})
				go func() {
					defer close(stopped)
					c.Stop("b")
				}()
				c.Partition("a", "b")
				<-stopped
				select {
				case d := <-downs:
					if d.Reason != grpcproc.ReasonShutdown && d.Reason != grpcproc.ReasonNoConnection {
						t.Fatalf("down: %+v", d)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("no Down")
				}
			})
		})
	}
}

// failTB records a Fatal instead of failing the test, and ends only the
// goroutine that called it, as Fatal does.
type failTB struct {
	testing.TB
	failed atomic.Bool
}

func (f *failTB) Helper()               {}
func (f *failTB) Fatal(...any)          { f.failed.Store(true); runtime.Goexit() }
func (f *failTB) Fatalf(string, ...any) { f.failed.Store(true); runtime.Goexit() }

// Two Restarts of one node at once: one starts it, the other fails, and no
// node is left running that the cluster does not know of.
func TestRestartTwiceAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var restarting atomic.Bool
		var arrived sync.WaitGroup
		gate := make(chan struct{})
		ft := &failTB{TB: t}
		c := grpcproctest.NewWith(ft, []grpcproctest.Option{grpcproctest.WithConfig(func(string, *grpcproc.Config) {
			if restarting.Load() {
				arrived.Done()
				<-gate
			}
		})}, "a")
		c.Stop("a")
		restarting.Store(true)
		arrived.Add(2)
		nodes := make(chan *grpcproc.Node, 2)
		var wg sync.WaitGroup
		for range 2 {
			wg.Go(func() { nodes <- c.Restart("a") }) // a failed Restart sends nothing
		}
		arrived.Wait() // both are past the check that a is not running
		close(gate)
		wg.Wait()
		close(nodes)
		var started []*grpcproc.Node
		for n := range nodes {
			started = append(started, n)
		}
		if len(started) != 1 || !ft.failed.Load() || c.Node("a") != started[0] {
			t.Fatalf("started %d nodes; the other failed: %v", len(started), ft.failed.Load())
		}
	})
}
