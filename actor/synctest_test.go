package actor_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
)

// Supervisors keep time on the bubble's clock: restart intensity and a
// worker's Shutdown are virtual seconds, which the test spends at once.
func TestSupervisorInSynctestBubble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = n.Stop(context.Background()) }()
		alive := func(pid grpcproc.PID) bool { _, ok := n.Process(pid); return ok }

		// Two restarts in 5s are allowed. Crashes 3s apart never make two
		// within any 5s; three at once do, and the supervisor gives up.
		crash := make(chan struct{})
		sup, err := actor.Supervise(n, actor.Spec{
			MaxRestarts: 2, Within: 5 * time.Second,
			Children: []actor.ChildSpec{actor.ChildFunc("worker", func(*grpcproc.Process[proto.Message]) error {
				<-crash
				return errors.New("boom")
			})},
		})
		if err != nil {
			t.Fatal(err)
		}
		for range 4 {
			crash <- struct{}{}
			synctest.Wait()
			time.Sleep(3 * time.Second)
		}
		if !alive(sup) {
			t.Fatal("the supervisor gave up on crashes 3s apart")
		}
		for range 3 {
			select {
			case crash <- struct{}{}: // none once the supervisor gave up
				synctest.Wait()
			default:
			}
		}
		if alive(sup) {
			t.Fatal("the supervisor outlived three crashes at once")
		}

		// A worker that ignores its exit holds its supervisor's stop for its
		// Shutdown, and the supervisor's end until it exits.
		release := make(chan struct{})
		sup, err = actor.Supervise(n, actor.Spec{
			Shutdown: 5 * time.Second,
			Children: []actor.ChildSpec{actor.ChildFunc("stuck", func(*grpcproc.Process[proto.Message]) error {
				<-release
				return nil
			})},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Exit(t.Context(), sup, "shutdown"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if !alive(sup) {
			t.Fatal("the supervisor ended before its stuck worker")
		}
		close(release)
		synctest.Wait()
		if alive(sup) {
			t.Fatal("the supervisor outlived its worker")
		}
	})
}
