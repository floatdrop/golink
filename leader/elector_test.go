package leader_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/leader"
	leaderv1 "github.com/floatdrop/grpcproc/leader/proto/grpcproc/leader/v1"
)

func elector(t *testing.T, c *grpcproctest.Cluster, node string) grpcproc.PID {
	t.Helper()
	pid, ok := c.Node(node).Whereis(leader.ElectorName("test"))
	if !ok {
		t.Fatalf("no elector on %s", node)
	}
	return pid
}

func inspect(t *testing.T, c *grpcproctest.Cluster, node string) map[string]string {
	t.Helper()
	m, err := c.Node(node).Inspect(t.Context(), elector(t, c, node))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func count(t *testing.T, c *grpcproctest.Cluster, node string, v int64) {
	t.Helper()
	got, err := grpcproc.Named[counter](node, "singleton").Call[counter](t.Context(), c.Node(node), wrapperspb.Int64(v))
	if err != nil || got.GetValue() != v {
		t.Fatalf("checkpoint %d on %s: %v, %v", v, node, got, err)
	}
}

func TestPartitionedLeaderStepsDown(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, term := elected(t, c, "a", "b", "c")
		rest := others(first)
		c.Partition(first, rest[0])
		c.Partition(first, rest[1])
		settle(2 * time.Second)
		second, term2 := elected(t, c, rest...)
		cut := status(t, c, first)
		if cut.Role == leader.Leader || cut.Term != term {
			t.Errorf("cut off, %s is %v in term %d, was leader in %d", first, cut.Role, cut.Term, term)
		}
		if got := inspect(t, c, first)["unreachable"]; got != strings.Join(rest, ",") {
			t.Errorf("unreachable from %s: %q", first, got)
		}
		c.Heal(first, rest[0])
		c.Heal(first, rest[1])
		settle(2 * time.Second)
		// Pre-votes kept the term of the node that was cut off where it was,
		// so coming back it deposes nobody.
		if again, term3 := elected(t, c, "a", "b", "c"); again != second || term3 != term2 {
			t.Errorf("after healing: %s in term %d, want %s in %d", again, term3, second, term2)
		}
		// The demoted singleton's last word was saved while cut off: nobody
		// heard it.
		want := []string{first + " starts from 0", first + " stops: demoted", second + " starts from 0"}
		slices.Sort(want)
		if got := j.sorted(); got != strings.Join(want, "; ") {
			t.Errorf("journal %q, want %q", got, want)
		}
		// Followers do not talk to each other: the one that was cut off
		// greets them, every GhostTTL, until it hears back.
		settle(6 * time.Second)
		if got, ok := inspect(t, c, first)["unreachable"]; ok {
			t.Errorf("still unreachable once healed: %s", got)
		}
	})
}

func TestResign(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, _ := elected(t, c, "a", "b", "c")
		count(t, c, first, 5)
		follower := others(first)[0]
		if err := leader.Resign(t.Context(), c.Node(follower), "test"); !errors.Is(err, leader.ErrNotLeader) {
			t.Errorf("Resign on a follower: %v", err)
		}
		if err := leader.Resign(t.Context(), c.Node(first), "test"); err != nil {
			t.Fatal(err)
		}
		settle(2 * time.Second)
		second, _ := elected(t, c, "a", "b", "c")
		if second == first {
			t.Errorf("%s leads again", first)
		}
		// The singleton's last word, saved as it stopped, reached the next.
		want := fmt.Sprintf("%s starts from 0; %s stops: demoted; %s starts from 105", first, first, second)
		if got := j.String(); got != want {
			t.Errorf("journal %q, want %q", got, want)
		}
	})
}

func TestResignAlone(t *testing.T) {
	alone := func(j *journal) leader.Spec[counter] {
		s := spec(j)
		s.Voters = []string{"a"}
		return s
	}
	nodes(t, []string{"a"}, []string{"a"}, func(j *journal, _ string) leader.Spec[counter] { return alone(j) }, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(time.Second)
		if info := status(t, c, "a"); info.Role != leader.Leader || info.Quorum != 1 {
			t.Fatalf("alone: %+v", info)
		}
		if err := leader.Resign(t.Context(), c.Node("a"), "test"); !errors.Is(err, leader.ErrNoSuccessor) {
			t.Errorf("Resign alone: %v", err)
		}
		settle(time.Second)
		if info := status(t, c, "a"); info.Role != leader.Leader {
			t.Errorf("not leading again after the backoff: %+v", info)
		}
	})
}

// A Resign waits for the singleton to stop, up to four election timeouts;
// meanwhile another Resign is refused.
func TestResignWaitsForTheSingleton(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		j.hold = make(chan struct{})
		settle(2 * time.Second)
		first, _ := elected(t, c, "a", "b", "c")
		done := make(chan error)
		go func() { done <- leader.Resign(context.Background(), c.Node(first), "test") }()
		settle(10 * time.Millisecond)
		if err := leader.Resign(t.Context(), c.Node(first), "test"); err == nil || !strings.Contains(err.Error(), "already resigning") {
			t.Errorf("a second Resign: %v", err)
		}
		if got := inspect(t, c, first)["singleton"]; got != "stopping" {
			t.Errorf("singleton %q", got)
		}
		settle(time.Second)
		if err := <-done; err != nil {
			t.Errorf("Resign: %v", err)
		}
		close(j.hold)
		settle(2 * time.Second)
		if second, _ := elected(t, c, "a", "b", "c"); second == first {
			t.Errorf("%s leads again", first)
		}
	})
}

// A leader that loses its majority while it resigns has resigned.
func TestResignInterruptedByAPartition(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		j.hold = make(chan struct{})
		settle(2 * time.Second)
		first, _ := elected(t, c, "a", "b", "c")
		done := make(chan error)
		go func() { done <- leader.Resign(context.Background(), c.Node(first), "test") }()
		settle(10 * time.Millisecond)
		for _, o := range others(first) {
			c.Partition(first, o)
		}
		if err := <-done; err != nil {
			t.Errorf("Resign: %v", err)
		}
		close(j.hold)
	})
}

func TestCheckpointOnceDemoted(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, _ := elected(t, c, "a", "b", "c")
		lease := j.lease(first)
		for _, o := range others(first) {
			c.Partition(first, o)
		}
		// Nobody acknowledges it: it waits, until the leader steps down.
		waiting := make(chan error)
		go func() { waiting <- lease.Checkpoint(context.Background(), wrapperspb.Int64(9)) }()
		settle(10 * time.Millisecond)
		if got := inspect(t, c, first)["checkpoints_waiting"]; got != "1" {
			t.Errorf("checkpoints waiting: %q", got)
		}
		if err := <-waiting; !errors.Is(err, leader.ErrNotLeader) {
			t.Errorf("Checkpoint as leadership ends: %v", err)
		}
		if err := lease.Checkpoint(t.Context(), wrapperspb.Int64(10)); !errors.Is(err, leader.ErrNotLeader) {
			t.Errorf("Checkpoint once demoted: %v", err)
		}
		lease.Save(wrapperspb.Int64(11)) // dropped
		if lease.Term() == 0 {
			t.Error("no term")
		}
	})
}

func TestConfirm(t *testing.T) {
	var mu sync.Mutex
	denied := 0
	confirming := func(j *journal) leader.Spec[counter] {
		s := spec(j)
		s.Confirm = func(ctx context.Context, term uint64) error {
			mu.Lock()
			defer mu.Unlock()
			if denied < 4 {
				denied++
				return errors.New("the lock is taken")
			}
			return nil
		}
		return s
	}
	cluster(t, confirming, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(time.Second)
		backingOff := 0
		for _, n := range []string{"a", "b", "c"} {
			if _, ok := inspect(t, c, n)["backoff"]; ok {
				backingOff++
			}
		}
		if backingOff == 0 {
			t.Error("nobody backs off after being denied")
		}
		settle(20 * time.Second)
		lead, _ := elected(t, c, "a", "b", "c")
		if got, want := j.String(), lead+" starts from 0"; got != want {
			t.Errorf("journal %q, want %q", got, want)
		}
	})
}

func TestSingletonThatFails(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, _ := elected(t, c, "a", "b", "c")
		count(t, c, first, 3)
		if err := grpcproc.Named[counter](first, "singleton").Send(t.Context(), c.Node(first), wrapperspb.Int64(-1)); err != nil {
			t.Fatal(err)
		}
		settle(5 * time.Second)
		elected(t, c, "a", "b", "c")
		events := strings.Split(j.String(), "; ")
		if len(events) != 3 || events[1] != first+" stops: told to fail" || !strings.HasSuffix(events[2], " starts from 3") {
			t.Errorf("journal %q", j)
		}
	})
}

func TestSingletonThatCannotStart(t *testing.T) {
	refusing := func(j *journal, node string) leader.Spec[counter] {
		s := spec(j)
		if node == "a" {
			s.Singleton = func(*leader.Lease[counter], counter) (actor.ChildSpec, error) {
				return actor.ChildSpec{}, errors.New("not here")
			}
		}
		return s
	}
	nodes(t, []string{"a", "b", "c"}, []string{"a", "b", "c"}, refusing, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		// However often a wins, b or c ends up leading.
		settle(30 * time.Second)
		if lead, _ := elected(t, c, "a", "b", "c"); lead == "a" {
			t.Error("a leads without a singleton")
		}
	})
}

// Alone, a node whose singleton cannot start wins every election, and
// backs off longer after each.
func TestSingletonThatNeverStarts(t *testing.T) {
	var mu sync.Mutex
	var attempts []time.Time
	refusing := func(j *journal, _ string) leader.Spec[counter] {
		s := spec(j)
		s.Voters = []string{"a"}
		s.Singleton = func(*leader.Lease[counter], counter) (actor.ChildSpec, error) {
			mu.Lock()
			defer mu.Unlock()
			attempts = append(attempts, time.Now())
			return actor.ChildSpec{}, errors.New("not here")
		}
		return s
	}
	tries := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts)
	}
	nodes(t, []string{"a"}, []string{"a"}, refusing, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		for tries() < 4 {
			settle(10 * time.Millisecond)
		}
		// Right after a failed attempt, it backs off.
		if _, ok := inspect(t, c, "a")["backoff"]; !ok {
			t.Error("no backoff")
		}
		mu.Lock()
		defer mu.Unlock()
		for i := 2; i < 4; i++ {
			if gap, before := attempts[i].Sub(attempts[i-1]), attempts[i-1].Sub(attempts[i-2]); gap <= before {
				t.Errorf("attempt %d came %v after the one before, which came %v after its own", i, gap, before)
			}
		}
	})
}

// A state a singleton cannot start from fails its start, as a Singleton
// that returns an error does.
func TestUndecodableState(t *testing.T) {
	for name, value := range map[string]*anypb.Any{
		"unknown type": {TypeUrl: "type.googleapis.com/no.such.Type"},
		"another type": mustAny(t, wrapperspb.String("seven")),
	} {
		t.Run(name, func(t *testing.T) {
			cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
				settle(2 * time.Second)
				first, term := elected(t, c, "a", "b", "c")
				if _, err := c.Node(first).CallTo[*emptypb.Empty](t.Context(), elector(t, c, first), &leaderv1.Checkpoint{Term: term, State: value}); err != nil {
					t.Fatal(err)
				}
				c.Kill(first)
				settle(3 * time.Second)
				for _, n := range others(first) {
					if _, ok := c.Node(n).Whereis("singleton"); ok {
						t.Errorf("%s started a singleton", n)
					}
				}
			})
		})
	}
}

func mustAny(t *testing.T, m proto.Message) *anypb.Any {
	a, err := anypb.New(m)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestLeaderStopsGracefully(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, _ := elected(t, c, "a", "b", "c")
		start := time.Now()
		c.Stop(first)
		// Its followers see its elector exit, and campaign at once rather
		// than wait out an election timeout.
		for {
			time.Sleep(10 * time.Millisecond)
			if info := status(t, c, others(first)[0]); info.Leader != "" && info.Leader != first {
				break
			}
		}
		if took := time.Since(start); took > 500*time.Millisecond {
			t.Errorf("a new leader took %v", took)
		}
	})
}

// A pre-vote counts only for the term its candidate would begin: a lone
// elector, which no other answers, takes one for another term as nothing,
// and one for its next term, with its own, as the majority it is.
func TestPreVoteCountsOnlyForTheTermItWouldBegin(t *testing.T) {
	nodes(t, []string{"a", "b", "c"}, []string{"a"}, func(j *journal, _ string) leader.Spec[counter] { return spec(j) },
		func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
			settle(time.Second) // a asks for pre-votes nobody answers
			preVote := func(term uint64) {
				t.Helper()
				msg := &leaderv1.Peer{Term: term, Kind: &leaderv1.Peer_Vote{Vote: &leaderv1.Vote{Pre: true, Granted: true}}}
				if err := c.Node("b").SendTo(t.Context(), grpcproc.Name{Node: "a", Name: leader.ElectorName("test")}, msg); err != nil {
					t.Fatal(err)
				}
				settle(time.Millisecond)
			}
			preVote(5)
			if info := status(t, c, "a"); info.Role != leader.Follower || info.Term != 0 {
				t.Errorf("after a pre-vote for term 5: %v in term %d, want a follower in term 0", info.Role, info.Term)
			}
			preVote(1)
			if info := status(t, c, "a"); info.Role != leader.Candidate || info.Term != 1 {
				t.Errorf("after a pre-vote for term 1: %v in term %d, want a candidate in term 1", info.Role, info.Term)
			}
		})
}

// A heartbeat of another term is not from this node's leader: a follower
// keeps the one it follows.
func TestHeartbeatOfAnotherTermIsNotFollowed(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		lead, term := elected(t, c, "a", "b", "c")
		f, g := others(lead)[0], others(lead)[1]
		msg := &leaderv1.Peer{Term: term - 1, Kind: &leaderv1.Peer_Heartbeat{Heartbeat: &leaderv1.Heartbeat{}}}
		if err := c.Node(g).SendTo(t.Context(), grpcproc.Name{Node: f, Name: leader.ElectorName("test")}, msg); err != nil {
			t.Fatal(err)
		}
		settle(time.Millisecond)
		if info := status(t, c, f); info.Leader != lead || info.Term != term {
			t.Errorf("%s follows %q in term %d, want %s in term %d", f, info.Leader, info.Term, lead, term)
		}
	})
}

// A follower cut off from its leader long enough to hold a pre-vote still
// follows it, so a hand-over to it goes through: TimeoutNow makes it
// campaign, and win.
func TestTimeoutNowReachesAPreCandidate(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, term := elected(t, c, "a", "b", "c")
		f := others(first)[0]
		c.Partition(first, f)
		settle(time.Second) // f asks for pre-votes, which the other follower, hearing first, ignores
		c.Heal(first, f)
		msg := &leaderv1.Peer{Term: term, Kind: &leaderv1.Peer_TimeoutNow{TimeoutNow: &leaderv1.TimeoutNow{}}}
		if err := c.Node(first).SendTo(t.Context(), grpcproc.Name{Node: f, Name: leader.ElectorName("test")}, msg); err != nil {
			t.Fatal(err)
		}
		settle(time.Second)
		if lead, _ := elected(t, c, "a", "b", "c"); lead != f {
			t.Errorf("%s leads, want %s, which first handed over to", lead, f)
		}
	})
}

// A TimeoutNow from a node that is not the leader is no hand-over: the
// follower that gets it does not campaign.
func TestTimeoutNowFromAnotherFollower(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, term := elected(t, c, "a", "b", "c")
		f, g := others(first)[0], others(first)[1]
		msg := &leaderv1.Peer{Term: term, Kind: &leaderv1.Peer_TimeoutNow{TimeoutNow: &leaderv1.TimeoutNow{}}}
		if err := c.Node(g).SendTo(t.Context(), grpcproc.Name{Node: f, Name: leader.ElectorName("test")}, msg); err != nil {
			t.Fatal(err)
		}
		settle(time.Second)
		if lead, term2 := elected(t, c, "a", "b", "c"); lead != first || term2 != term {
			t.Errorf("%s leads in term %d, after %s in %d", lead, term2, first, term)
		}
	})
}

// A leader that hears of a newer term steps down.
func TestNewerTerm(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, term := elected(t, c, "a", "b", "c")
		other := others(first)[0]
		msg := &leaderv1.Peer{Term: term + 5, Kind: &leaderv1.Peer_Ack{Ack: &leaderv1.Ack{}}}
		if err := c.Node(other).SendTo(t.Context(), grpcproc.Name{Node: first, Name: leader.ElectorName("test")}, msg); err != nil {
			t.Fatal(err)
		}
		settle(2 * time.Second)
		if _, term2 := elected(t, c, "a", "b", "c"); term2 <= term+5 {
			t.Errorf("term %d, after %d", term2, term+5)
		}
		if !strings.Contains(j.String(), first+" stops: demoted") {
			t.Errorf("journal %q", j)
		}
	})
}

func TestElectorAnswersWhatItKnows(t *testing.T) {
	voters := func(j *journal, _ string) leader.Spec[counter] { return spec(j) }
	nodes(t, []string{"a", "b", "c", "d"}, []string{"a", "b", "c"}, voters, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, term := elected(t, c, "a", "b", "c")
		info := inspect(t, c, first)
		was := elector(t, c, first)
		pid, _ := c.Node(first).Whereis("singleton")
		want := map[string]string{
			"role": "leader", "term": fmt.Sprint(term), "leader": first, "voted_for": first,
			"view": "a,b,c", "quorum": "2", "state": fmt.Sprintf("%d.1", term), "singleton": pid.String(),
		}
		for k, v := range want {
			if info[k] != v {
				t.Errorf("%s = %q, want %q", k, info[k], v)
			}
		}
		if _, err := c.Node(first).CallTo[*emptypb.Empty](t.Context(), elector(t, c, first), &emptypb.Empty{}); err == nil || !strings.Contains(err.Error(), "does not answer google.protobuf.Empty") {
			t.Errorf("an unknown call: %v", err)
		}
		for _, m := range []proto.Message{&emptypb.Empty{}, &leaderv1.PeerDown{Node: "nobody", Reason: "normal"}} {
			if err := c.Node(first).SendTo(t.Context(), elector(t, c, first), m); err != nil {
				t.Fatal(err)
			}
		}
		// Elector messages from a node that is not a voter are not heard.
		if err := c.Node("d").SendTo(t.Context(), grpcproc.Name{Node: first, Name: leader.ElectorName("test")}, &leaderv1.Peer{Term: 1000}); err != nil {
			t.Fatal(err)
		}
		settle(time.Second)
		if _, term2 := elected(t, c, "a", "b", "c"); term2 != term {
			t.Errorf("term %d, was %d", term2, term)
		}
		if again := elector(t, c, first); again != was {
			t.Error("the elector restarted")
		}
		if _, err := leader.Status(t.Context(), c.Node(first), "other"); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Errorf("Status of a cluster nobody runs: %v", err)
		}
	})
	if got := leader.Role(9).String(); got != "Role(9)" {
		t.Errorf("Role(9) = %q", got)
	}
}

// members is a Membership the test drives.
type members struct {
	mu       sync.Mutex
	failures int // Watch fails this many times first
	watches  []chan grpcproc.MemberEvent
	initial  []string
}

func (m *members) Watch(ctx context.Context) (<-chan grpcproc.MemberEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failures > 0 {
		m.failures--
		return nil, errors.New("etcd is down")
	}
	ch := make(chan grpcproc.MemberEvent, 16)
	for _, n := range m.initial {
		ch <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: n}, Up: true}
	}
	m.watches = append(m.watches, ch)
	go func() {
		<-ctx.Done()
		m.end(ch)
	}()
	return ch, nil
}

// emit reports ev to every watch.
func (m *members) emit(node string, up bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if up {
		m.initial = append(m.initial, node)
	} else {
		m.initial = slices.DeleteFunc(m.initial, func(n string) bool { return n == node })
	}
	for _, ch := range m.watches {
		ch <- grpcproc.MemberEvent{Member: grpcproc.Member{Name: node}, Up: up}
	}
}

// end ends ch, or every watch.
func (m *members) end(ch chan grpcproc.MemberEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.watches = slices.DeleteFunc(m.watches, func(w chan grpcproc.MemberEvent) bool {
		if ch == nil || w == ch {
			close(w)
			return true
		}
		return false
	})
}

func view(t *testing.T, c *grpcproctest.Cluster, node string) string {
	t.Helper()
	info := status(t, c, node)
	return info.Role.String() + " " + strings.Join(info.View, ",")
}

func TestDynamicViewFromPeers(t *testing.T) {
	// a and b know of each other, and c of both.
	dynamic := func(j *journal, node string) leader.Spec[counter] {
		s := spec(j)
		s.Voters, s.Peers = nil, []string{"a", "b"}
		if node == "a" {
			s.Peers = append(s.Peers, "c")
		}
		return s
	}
	nodes(t, []string{"a", "b", "c"}, []string{"a", "b"}, dynamic, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(time.Second)
		// c runs no elector: it is out of a's view, which, as b's, is too
		// small. c's greeting is how b comes to know it.
		for _, n := range []string{"a", "b"} {
			if got := view(t, c, n); got != "unclustered a,b" {
				t.Errorf("%s: %s", n, got)
			}
		}
		if _, err := leader.Start(c.Node("c"), dynamic(j, "c")); err != nil {
			t.Fatal(err)
		}
		settle(2 * time.Second)
		lead, _ := elected(t, c, "a", "b", "c")

		// A node that cannot be reached is a ghost for GhostTTL, then leaves
		// the view; heard from again, it is back.
		ghost := others(lead)[0]
		c.Partition(lead, ghost)
		settle(time.Second)
		if got := view(t, c, lead); got != "leader a,b,c" {
			t.Errorf("with a ghost: %s", got)
		}
		settle(5 * time.Second)
		if got := view(t, c, lead); got != "unclustered "+strings.Join(others(ghost), ",") {
			t.Errorf("once the ghost is gone: %s", got)
		}
		c.Heal(lead, ghost)
		settle(6 * time.Second)
		elected(t, c, "a", "b", "c")

		// A node whose elector exits leaves the view at once.
		c.Stop(ghost)
		settle(time.Second)
		for _, n := range others(ghost) {
			if got := view(t, c, n); got != "unclustered "+strings.Join(others(ghost), ",") {
				t.Errorf("%s once %s stopped: %s", n, ghost, got)
			}
		}
	})
}

// A node Membership reports down while it cannot be reached is gone from
// the view: once it is back and talks, it is not listened to, nor greeted,
// until Membership reports it up again.
func TestReportedDownWhileUnreachable(t *testing.T) {
	m := &members{initial: []string{"a", "b", "c"}}
	dynamic := func(j *journal, _ string) leader.Spec[counter] {
		s := spec(j)
		s.Voters, s.Membership, s.MinClusterSize = nil, m, 2
		return s
	}
	nodes(t, []string{"a", "b", "c"}, []string{"a", "b", "c"}, dynamic, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		lead, _ := elected(t, c, "a", "b", "c")
		x := others(lead)[0]
		rest := []string{lead, others(lead)[1]}
		slices.Sort(rest)
		c.Partition(lead, x)
		settle(time.Second) // x is a ghost to the leader
		m.emit(x, false)
		settle(time.Second)
		want := "leader " + strings.Join(rest, ",")
		if got := view(t, c, lead); got != want {
			t.Errorf("with %s reported down: %s, want %s", x, got, want)
		}
		c.Heal(lead, x)
		settle(time.Second) // x talks again
		if got := view(t, c, lead); got != want {
			t.Errorf("with %s back but not reported: %s, want %s", x, got, want)
		}
		m.emit(x, true)
		settle(time.Second)
		if got := view(t, c, lead); got != "leader a,b,c" {
			t.Errorf("with %s reported up: %s", x, got)
		}
	})
}

// Without a Membership of its own, an elector follows its node's: c runs an
// elector and talks, but the nodes' Membership has not reported it, so a
// and b do not listen to it. Without any Membership, it would join.
func TestMembershipDefaultsToTheNodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := &members{initial: []string{"a", "b"}}
		c := grpcproctest.NewWith(t, []grpcproctest.Option{grpcproctest.WithConfig(func(_ string, cfg *grpcproc.Config) {
			cfg.Membership = m
		})}, "a", "b", "c")
		j := &journal{}
		for _, name := range []string{"a", "b", "c"} {
			s := spec(j)
			s.Voters, s.MinClusterSize = nil, 2
			if _, err := leader.Start(c.Node(name), s); err != nil {
				t.Fatal(err)
			}
		}
		settle(2 * time.Second)
		lead, _ := elected(t, c, "a", "b")
		if got := view(t, c, lead); got != "leader a,b" {
			t.Errorf("view %q, want the node's Membership's", got)
		}
	})
}

func TestDynamicViewFromMembership(t *testing.T) {
	m := &members{failures: 3, initial: []string{"a", "b"}}
	dynamic := func(j *journal, _ string) leader.Spec[counter] {
		s := spec(j)
		s.Voters, s.Membership, s.MinClusterSize = nil, m, 2
		return s
	}
	nodes(t, []string{"a", "b", "c"}, []string{"a", "b", "c"}, dynamic, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		// The watch fails at first, and is tried again every GhostTTL. c
		// runs an elector, but Membership has not reported it: a and b do
		// not listen to it.
		settle(12 * time.Second)
		lead, _ := elected(t, c, "a", "b")
		if got := view(t, c, lead); got != "leader a,b" {
			t.Errorf("before c is reported: %s", got)
		}
		m.emit("c", true)
		settle(time.Second)
		elected(t, c, "a", "b", "c")
		if got := view(t, c, lead); got != "leader a,b,c" {
			t.Errorf("with c reported: %s", got)
		}

		// Reported down, a node leaves every view, and is not listened to
		// though it still talks; up again, it is back.
		m.emit("c", false)
		settle(time.Second)
		if got := view(t, c, lead); got != "leader a,b" {
			t.Errorf("with c reported down: %s", got)
		}
		if again, _ := elected(t, c, "a", "b"); again != lead {
			t.Errorf("%s leads, was %s", again, lead)
		}
		m.emit("c", true)
		settle(time.Second)
		if got := view(t, c, lead); got != "leader a,b,c" {
			t.Errorf("with c back: %s", got)
		}

		// A watch that ends is started again.
		m.end(nil)
		settle(6 * time.Second)
		m.mu.Lock()
		watching := len(m.watches)
		m.mu.Unlock()
		if watching != 3 {
			t.Errorf("%d watches", watching)
		}
	})
}

func TestChild(t *testing.T) {
	nodes(t, []string{"a", "d"}, nil, nil, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		s := spec(j)
		s.Voters = []string{"a"}
		child, err := leader.Child("election", s)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := actor.Supervise(c.Node("a"), actor.Spec{Children: []actor.ChildSpec{child}}); err != nil {
			t.Fatal(err)
		}
		settle(time.Second)
		if got := j.String(); got != "a starts from 0" {
			t.Errorf("journal %q", got)
		}
		// On a node that is not a voter, the elector fails to start, until
		// its supervisor gives up.
		sup, err := actor.Supervise(c.Node("d"), actor.Spec{Children: []actor.ChildSpec{child}})
		if err != nil {
			t.Fatal(err)
		}
		settle(time.Second)
		if _, ok := c.Node("d").Process(sup); ok {
			t.Error("the supervisor of an elector that cannot run still runs")
		}
	})
}

func TestSpecs(t *testing.T) {
	ok := func(*leader.Lease[counter], counter) (actor.ChildSpec, error) { return actor.ChildSpec{}, nil }
	for want, s := range map[string]leader.Spec[counter]{
		"needs a Cluster":                 {Singleton: ok},
		"needs a Singleton":               {Cluster: "x"},
		"not both":                        {Cluster: "x", Singleton: ok, Voters: []string{"a"}, Peers: []string{"b"}},
		"among the Voters twice":          {Cluster: "x", Singleton: ok, Voters: []string{"a", "b", "a"}},
		"a negative MinClusterSize":       {Cluster: "x", Singleton: ok, MinClusterSize: -1},
		"shorter than ElectionTimeout":    {Cluster: "x", Singleton: ok, HeartbeatInterval: time.Second},
		"must be positive":                {Cluster: "x", Singleton: ok, HeartbeatInterval: -1},
		"a negative MinClusterSize, Elec": {Cluster: "x", Singleton: ok, GhostTTL: -1},
		"is not among the Voters":         {Cluster: "x", Singleton: ok, Voters: []string{"b"}},
	} {
		n, err := grpcproc.NewNode(grpcproc.Config{Name: "a", Resolver: grpcproc.StaticResolver{}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := leader.Start(n, s); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Start: %v, want %q", err, want)
		}
		if want != "is not among the Voters" {
			if _, err := leader.Child("x", s); err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("Child: %v, want %q", err, want)
			}
		}
	}
}

// A state that cannot be encoded is Checkpoint's error, and Save's log.
func TestStateThatCannotBeEncoded(t *testing.T) {
	type text = *wrapperspb.StringValue
	got := make(chan error, 1)
	s := leader.Spec[text]{Cluster: "test", Voters: []string{"a"}, Singleton: func(l *leader.Lease[text], _ text) (actor.ChildSpec, error) {
		return actor.ChildFunc("singleton", func(p *grpcproc.Process[text]) error {
			bad := wrapperspb.String("\xff")
			l.Save(bad)
			got <- l.Checkpoint(p.Context(), bad)
			_, err := p.Receive()
			return err
		}), nil
	}}
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		if _, err := leader.Start(c.Node("a"), s); err != nil {
			t.Fatal(err)
		}
		settle(time.Second)
		if err := <-got; err == nil || !strings.Contains(err.Error(), "UTF-8") {
			t.Errorf("Checkpoint: %v", err)
		}
	})
}

// With S an interface, any state will do.
func TestAnyState(t *testing.T) {
	got := make(chan string, 2)
	s := leader.Spec[proto.Message]{Cluster: "test", Voters: []string{"a"}, Singleton: func(l *leader.Lease[proto.Message], state proto.Message) (actor.ChildSpec, error) {
		return actor.ChildFunc("singleton", func(p *grpcproc.Process[proto.Message]) error {
			if state != nil {
				got <- string(proto.MessageName(state).Name()) + " " + fmt.Sprint(state.(*wrapperspb.StringValue).GetValue())
			}
			if err := l.Checkpoint(p.Context(), wrapperspb.String("hello")); err != nil {
				return err
			}
			return errors.New("done") // and again, from the state it left
		}), nil
	}}
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		if _, err := leader.Start(c.Node("a"), s); err != nil {
			t.Fatal(err)
		}
		settle(2 * time.Second)
		if s := <-got; s != "StringValue hello" {
			t.Errorf("state %q", s)
		}
	})
}
