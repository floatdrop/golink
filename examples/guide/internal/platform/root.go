package platform

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
)

// Root is the node's top supervisor. Its children are the services' trees,
// which the services' modules add to the group of actor.ChildSpec: a
// program runs the services its entry point composes, and nothing here
// knows which those are.
type Root struct {
	node *grpcproc.Node
	spec actor.Spec
	pid  grpcproc.PID

	gone   chan struct{} // closed once the root has exited
	reason string        // why it did; read after gone is closed
	cancel context.CancelFunc
}

func newRoot(n *grpcproc.Node, trees []actor.ChildSpec) *Root {
	return &Root{node: n, spec: actor.Spec{Strategy: actor.OneForOne, Children: trees}}
}

// start runs the trees, in the order their modules were composed, and only
// then starts the node: with a registry, that is when peers learn of it, so
// none routes to a service that does not exist yet.
func (r *Root) start(ctx context.Context) error {
	// Subscribed before the root exists, so its exit cannot be missed.
	watch, cancel := context.WithCancel(context.Background())
	events := r.node.Subscribe(watch, 64)
	pid, err := actor.Supervise(r.node, r.spec, grpcproc.WithName("root"))
	if err != nil {
		cancel()
		return err
	}
	r.pid, r.gone, r.cancel = pid, make(chan struct{}), cancel
	go r.watch(events)
	if err := r.node.Start(ctx); err != nil {
		// No stop hook runs for a start that failed: stop the trees here.
		return errors.Join(err, r.stop(ctx))
	}
	return nil
}

// watch closes gone when the root exits, with the reason.
func (r *Root) watch(events <-chan grpcproc.Event) {
	defer close(r.gone)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case e, ok := <-events:
			if !ok {
				r.reason = grpcproc.ReasonShutdown // the node stopped
				return
			}
			if e.Kind == grpcproc.EventExit && e.Process.PID == r.pid {
				r.reason = e.Reason
				return
			}
		case <-tick.C:
			// A node busy enough to drop events may drop this one, and if
			// nothing follows it no later event says so. A root that is gone
			// with nothing left to read went unreported.
			if _, running := r.node.Process(r.pid); !running && len(events) == 0 {
				r.reason = "exited"
				return
			}
		}
	}
}

// wait is the root's worker: it returns when the root exits. The container
// cancels it before stopping the root, so an exit it sees is the node's
// services giving up, and its error stops the program, for whatever runs it
// to start again.
func (r *Root) wait(ctx context.Context) error {
	select {
	case <-r.gone:
		return fmt.Errorf("platform: the root supervisor exited: %s", r.reason)
	case <-ctx.Done():
		return nil
	}
}

// stop asks the root to exit and waits until it has. A supervisor stops
// its children first, in reverse order, so every service is down before the
// node is stopped under it.
func (r *Root) stop(ctx context.Context) error {
	defer r.cancel()
	if err := r.node.Exit(ctx, r.pid, grpcproc.ReasonShutdown); err != nil {
		return err
	}
	select {
	case <-r.gone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
