// Package singleton is a job that runs once a minute in a three-node
// cluster, on whichever node leads: grpcproc/cron as grpcproc/leader's
// singleton.
package singleton

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/cron"
	cronv1 "github.com/floatdrop/grpcproc/cron/proto/grpcproc/cron/v1"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/leader"
)

// election is every node's part: the leader runs a cron process, which
// saves each job's last run as the singleton's state, and the next leader's
// resumes from it.
func election(jobs []cron.Job) leader.Spec[*cronv1.State] {
	return leader.Spec[*cronv1.State]{
		Cluster: "cron",
		Voters:  []string{"a", "b", "c"},
		Singleton: func(l *leader.Lease[*cronv1.State], last *cronv1.State) (actor.ChildSpec, error) {
			return cron.Child("cron", cron.Spec{Jobs: jobs, Resume: last, OnState: l.Save})
		},
	}
}

func TestOnceAMinuteAcrossAFailover(t *testing.T) {
	// A bubble: its clock is fake, and moves on when every goroutine in it
	// waits, so minutes pass at once.
	synctest.Test(t, func(t *testing.T) {
		sleepUntil("10:07:30")
		var mu sync.Mutex
		ran := map[string]string{} // minute: the node it ran on
		report := cron.Job{
			Name: "report",
			Spec: "* * * * *",
			// Were the next leader elected more than a minute late, it
			// would still start the run it missed.
			StartingDeadline: 5 * time.Minute,
			Action: func(p *grpcproc.Process[proto.Message], r cron.Run) error {
				mu.Lock()
				defer mu.Unlock()
				minute := r.Time.Format("15:04")
				if node, twice := ran[minute]; twice {
					t.Errorf("%s ran on %s, and on %s", minute, node, p.Node().Name())
				}
				ran[minute] = p.Node().Name()
				return nil
			},
		}
		c := grpcproctest.New(t, "a", "b", "c")
		for _, node := range []string{"a", "b", "c"} {
			if _, err := leader.Start(c.Node(node), election([]cron.Job{report})); err != nil {
				t.Fatal(err)
			}
		}

		// The leader crashes a moment before 10:10: the next is elected
		// once that minute has begun, and starts the run it owes.
		sleepUntil("10:09:59.9")
		info, err := leader.Status(t.Context(), c.Node("a"), "cron")
		if err != nil {
			t.Fatal(err)
		}
		first := info.Leader
		c.Kill(first)
		sleepUntil("10:12:30")

		mu.Lock()
		defer mu.Unlock()
		for _, minute := range []string{"10:08", "10:09", "10:10", "10:11", "10:12"} {
			node, ok := ran[minute]
			switch {
			case !ok:
				t.Errorf("%s did not run", minute)
			case (minute < "10:10") != (node == first):
				t.Errorf("%s ran on %s; %s led until 10:10", minute, node, first)
			}
		}
	})
}

// sleepUntil moves the bubble's clock to a time of September 28, 2026, and
// lets everything settle.
func sleepUntil(clock string) {
	at, err := time.Parse("2006-01-02 15:04:05", "2026-09-28 "+clock)
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Until(at))
	synctest.Wait()
}
