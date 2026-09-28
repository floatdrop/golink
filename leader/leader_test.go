package leader_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/leader"
)

type counter = *wrapperspb.Int64Value

// journal records what the singletons of a cluster do, on every node.
type journal struct {
	mu     sync.Mutex
	events []string
	leases map[string]*leader.Lease[counter]
	// hold, if set, holds a demoted singleton's end until it is closed.
	hold chan struct{}
}

func (j *journal) add(format string, args ...any) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.events = append(j.events, fmt.Sprintf(format, args...))
}

func (j *journal) String() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return strings.Join(j.events, "; ")
}

// sorted is the journal in order, for events whose order is a race.
func (j *journal) sorted() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return strings.Join(slices.Sorted(slices.Values(j.events)), "; ")
}

func (j *journal) lease(node string) *leader.Lease[counter] {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.leases[node]
}

// singleton is a Singleton that records its start, with the state it got,
// and its end, and checkpoints what it is sent: an Int64Value.
func (j *journal) singleton(l *leader.Lease[counter], state counter) (actor.ChildSpec, error) {
	return actor.ChildFunc("singleton", func(p *grpcproc.Process[counter]) error {
		node := p.Node().Name()
		j.mu.Lock()
		if j.leases == nil {
			j.leases = map[string]*leader.Lease[counter]{}
		}
		j.leases[node] = l
		j.mu.Unlock()
		j.add("%s starts from %d", node, state.GetValue())
		last := state.GetValue()
		for {
			m, err := p.Receive()
			if err != nil {
				reason := err.Error()
				if ee, ok := errors.AsType[*grpcproc.ExitError](err); ok {
					reason = ee.Reason
				}
				j.add("%s stops: %s", node, reason)
				if reason == leader.ReasonDemoted {
					// What a Terminate would do: save the last word.
					l.Save(wrapperspb.Int64(last + 100))
					if j.hold != nil {
						<-j.hold
					}
				}
				return err
			}
			if m.Body.GetValue() < 0 {
				j.add("%s stops: told to fail", node)
				return errors.New("told to fail")
			}
			last = m.Body.GetValue()
			err = l.Checkpoint(p.Context(), m.Body)
			if m.IsCall() {
				_ = m.Reply(m.Body, err)
			}
		}
	}), nil
}

func spec(j *journal) leader.Spec[counter] {
	return leader.Spec[counter]{
		Cluster:   "test",
		Voters:    []string{"a", "b", "c"},
		Singleton: j.singleton,
	}
}

// cluster runs f in a synctest bubble, with a cluster of nodes a, b and c
// that each run their part in s's election.
func cluster(t *testing.T, s func(*journal) leader.Spec[counter], f func(t *testing.T, c *grpcproctest.Cluster, j *journal)) {
	t.Helper()
	nodes(t, []string{"a", "b", "c"}, []string{"a", "b", "c"}, func(j *journal, _ string) leader.Spec[counter] { return s(j) }, f)
}

// nodes runs f in a synctest bubble, with a cluster of the nodes all, of
// which those in run take part in the election s describes for each.
func nodes(t *testing.T, all, run []string, s func(j *journal, node string) leader.Spec[counter], f func(t *testing.T, c *grpcproctest.Cluster, j *journal)) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		j := &journal{}
		c := grpcproctest.New(t, all...)
		for _, name := range run {
			if _, err := leader.Start(c.Node(name), s(j, name)); err != nil {
				t.Fatal(err)
			}
		}
		f(t, c, j)
	})
}

func settle(d time.Duration) {
	time.Sleep(d)
	synctest.Wait()
}

func status(t *testing.T, c *grpcproctest.Cluster, node string) leader.Info {
	t.Helper()
	info, err := leader.Status(t.Context(), c.Node(node), "test")
	if err != nil {
		t.Fatalf("%s: %v", node, err)
	}
	return info
}

// elected is the one leader the given nodes agree on, in one term.
func elected(t *testing.T, c *grpcproctest.Cluster, nodes ...string) (string, uint64) {
	t.Helper()
	var leaders []string
	var who []string
	var terms []uint64
	for _, n := range nodes {
		info := status(t, c, n)
		if info.Role == leader.Leader {
			leaders = append(leaders, n)
		}
		who = append(who, info.Leader)
		terms = append(terms, info.Term)
	}
	if len(leaders) != 1 || len(slices.Compact(who)) != 1 || who[0] != leaders[0] || len(slices.Compact(terms)) != 1 {
		t.Fatalf("no single leader: leaders %v, believed %v, terms %v", leaders, who, terms)
	}
	return leaders[0], terms[0]
}

func others(of string) []string {
	return slices.DeleteFunc([]string{"a", "b", "c"}, func(n string) bool { return n == of })
}

func TestElectsOneLeader(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		lead, _ := elected(t, c, "a", "b", "c")
		for _, n := range []string{"a", "b", "c"} {
			info := status(t, c, n)
			pid, ok := c.Node(n).Whereis("singleton")
			if ok != (n == lead) {
				t.Errorf("%s runs a singleton: %v", n, ok)
			}
			if n == lead && info.Singleton != pid {
				t.Errorf("Status says the singleton is %v, it is %v", info.Singleton, pid)
			}
			if !slices.Equal(info.View, []string{"a", "b", "c"}) || info.Quorum != 2 {
				t.Errorf("%s: view %v, quorum %d", n, info.View, info.Quorum)
			}
		}
		if got, want := j.String(), lead+" starts from 0"; got != want {
			t.Errorf("journal %q, want %q", got, want)
		}
	})
}

func TestFailover(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, term := elected(t, c, "a", "b", "c")
		one := grpcproc.Named[counter](first, "singleton")
		if _, err := c.Node(first).Call[counter](t.Context(), one, wrapperspb.Int64(7)); err != nil {
			t.Fatal(err)
		}
		c.Kill(first)
		settle(2 * time.Second)
		second, term2 := elected(t, c, others(first)...)
		if term2 <= term {
			t.Errorf("term %d after %d", term2, term)
		}
		// Killed, the node cancels its processes; whether the singleton
		// sees that first or its supervisor stopping it is a race.
		events := strings.Split(j.String(), "; ")
		if len(events) != 3 || events[0] != first+" starts from 0" || !strings.HasPrefix(events[1], first+" stops: ") || events[2] != second+" starts from 7" {
			t.Errorf("journal %q", j)
		}
	})
}
