package leader_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/leader"
	leaderv1 "github.com/floatdrop/grpcproc/leader/proto/grpcproc/leader/v1"
)

// cordoned is what every given node's elector says is cordoned.
func cordoned(t *testing.T, c *grpcproctest.Cluster, nodes ...string) string {
	t.Helper()
	var seen []string
	for _, n := range nodes {
		seen = append(seen, strings.Join(status(t, c, n).Cordoned, ","))
	}
	if len(slices.Compact(slices.Clone(seen))) != 1 {
		t.Fatalf("the nodes disagree on who is cordoned: %q", seen)
	}
	return seen[0]
}

// restart starts node again, as a new incarnation, and its elector.
func restart(t *testing.T, c *grpcproctest.Cluster, j *journal, node string) {
	t.Helper()
	c.Restart(node)
	if _, err := leader.Start(c.Node(node), spec(j)); err != nil {
		t.Fatal(err)
	}
}

func TestCordon(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, _ := elected(t, c, "a", "b", "c")
		rest := others(first)
		f, g := rest[0], rest[1]

		// From a follower's node: it goes to the leader.
		if err := leader.Cordon(t.Context(), c.Node(f), "test", f); err != nil {
			t.Fatal(err)
		}
		if err := leader.Cordon(t.Context(), c.Node(first), "test", f); err != nil {
			t.Fatalf("cordoned twice: %v", err)
		}
		settle(time.Second)
		if got := cordoned(t, c, "a", "b", "c"); got != f {
			t.Errorf("cordoned %q, want %s", got, f)
		}
		if got := inspect(t, c, first)["cordoned"]; got != f {
			t.Errorf("inspected cordoned %q", got)
		}

		// With the leader gone, only the node not cordoned can lead.
		c.Kill(first)
		settle(2 * time.Second)
		if lead, _ := elected(t, c, rest...); lead != g {
			t.Errorf("%s leads; %s is cordoned", lead, f)
		}

		if err := leader.Uncordon(t.Context(), c.Node(g), "test", f); err != nil {
			t.Fatal(err)
		}
		if err := leader.Uncordon(t.Context(), c.Node(g), "test", f); err != nil {
			t.Fatalf("uncordoned twice: %v", err)
		}
		settle(time.Second)
		if got := cordoned(t, c, rest...); got != "" {
			t.Errorf("still cordoned: %q", got)
		}
	})
}

func TestCordonedLeaderHandsOver(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, _ := elected(t, c, "a", "b", "c")
		count(t, c, first, 4)
		if err := leader.Cordon(t.Context(), c.Node(first), "test", first); err != nil {
			t.Fatal(err)
		}
		settle(5 * time.Second)
		second, _ := elected(t, c, "a", "b", "c")
		if second == first {
			t.Fatalf("%s leads, cordoned", first)
		}
		// It handed over as Resign does, its singleton's last word included.
		want := fmt.Sprintf("%s starts from 0; %s stops: demoted; %s starts from 104", first, first, second)
		if got := j.String(); got != want {
			t.Errorf("journal %q, want %q", got, want)
		}
	})
}

// A node restarted while cordoned holds nothing, and learns it is cordoned
// from the leader.
func TestCordonOutlastsARestart(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, _ := elected(t, c, "a", "b", "c")
		f, g := others(first)[0], others(first)[1]
		if err := leader.Cordon(t.Context(), c.Node(first), "test", f); err != nil {
			t.Fatal(err)
		}
		c.Kill(f)
		restart(t, c, j, f)
		settle(2 * time.Second)
		if got := cordoned(t, c, "a", "b", "c"); got != f {
			t.Errorf("cordoned %q after %s restarted", got, f)
		}
		c.Kill(first)
		settle(2 * time.Second)
		if lead, _ := elected(t, c, f, g); lead != g {
			t.Errorf("%s leads; %s is cordoned", lead, f)
		}
	})
}

// With every other node cordoned, a leader that restarts holds nothing,
// and the cordoned nodes will not vote for its older state; they send it
// theirs with their refusals, and it wins with that.
func TestRestartedNodeCatchesUp(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, _ := elected(t, c, "a", "b", "c")
		count(t, c, first, 5)
		for _, o := range others(first) {
			if err := leader.Cordon(t.Context(), c.Node(first), "test", o); err != nil {
				t.Fatal(err)
			}
		}
		err := leader.Cordon(t.Context(), c.Node(first), "test", first)
		if err == nil || !strings.Contains(err.Error(), "would leave no node of the view to lead") {
			t.Errorf("cordoning the last node that may lead: %v", err)
		}
		c.Kill(first)
		restart(t, c, j, first)
		settle(3 * time.Second)
		if lead, _ := elected(t, c, "a", "b", "c"); lead != first {
			t.Fatalf("%s leads", lead)
		}
		if events := strings.Split(j.String(), "; "); events[len(events)-1] != first+" starts from 5" {
			t.Errorf("journal %q", j)
		}
	})
}

func TestTransfer(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		// Nobody leads during the first election timeouts.
		if err := leader.Transfer(t.Context(), c.Node("a"), "test", "b"); !errors.Is(err, leader.ErrNoLeader) {
			t.Errorf("Transfer before an election: %v", err)
		}
		settle(2 * time.Second)
		first, _ := elected(t, c, "a", "b", "c")
		f, g := others(first)[0], others(first)[1]
		count(t, c, first, 6)
		for to, want := range map[string]string{
			first: first + " leads already",
			"zzz": "zzz is not in the view",
		} {
			if err := leader.Transfer(t.Context(), c.Node(f), "test", to); err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("Transfer to %q: %v, want %q", to, err, want)
			}
		}
		if err := leader.Cordon(t.Context(), c.Node(f), "test", f); err != nil {
			t.Fatal(err)
		}
		if err := leader.Transfer(t.Context(), c.Node(f), "test", f); err == nil || !strings.Contains(err.Error(), f+" is cordoned") {
			t.Errorf("Transfer to a cordoned node: %v", err)
		}
		if err := leader.Transfer(t.Context(), c.Node(f), "test", g); err != nil {
			t.Fatal(err)
		}
		settle(2 * time.Second)
		if lead, _ := elected(t, c, "a", "b", "c"); lead != g {
			t.Errorf("%s leads, want %s", lead, g)
		}
		if events := strings.Split(j.String(), "; "); events[len(events)-1] != g+" starts from 106" {
			t.Errorf("journal %q", j)
		}
	})
}

// A transfer to a follower the leader cannot reach finds nobody to hand
// over to.
func TestTransferToAnUnreachableFollower(t *testing.T) {
	cluster(t, spec, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, _ := elected(t, c, "a", "b", "c")
		f, g := others(first)[0], others(first)[1]
		c.Partition(first, g)
		settle(time.Second)
		if lead, _ := elected(t, c, first, f); lead != first {
			t.Fatalf("%s leads", lead)
		}
		if err := leader.Transfer(t.Context(), c.Node(first), "test", g); !errors.Is(err, leader.ErrNoSuccessor) {
			t.Errorf("Transfer to a follower cut off: %v", err)
		}
	})
}

func TestCordonRefusals(t *testing.T) {
	voters := func(j *journal, _ string) leader.Spec[counter] { return spec(j) }
	nodes(t, []string{"a", "b", "c", "d"}, []string{"a", "b", "c"}, voters, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		first, _ := elected(t, c, "a", "b", "c")
		follower := others(first)[0]
		if err := leader.Cordon(t.Context(), c.Node(first), "test", "zzz"); err == nil || !strings.Contains(err.Error(), "zzz takes no part in this election") {
			t.Errorf("cordoning a stranger: %v", err)
		}
		if err := leader.Cordon(t.Context(), c.Node("d"), "test", first); !errors.Is(err, grpcproc.ErrNoProc) {
			t.Errorf("Cordon from a node with no elector: %v", err)
		}
		// A follower's elector does not change who may lead.
		_, err := c.Node(first).CallTo[*emptypb.Empty](t.Context(), elector(t, c, follower), &leaderv1.Cordon{Node: follower})
		if err == nil || !strings.Contains(err.Error(), leader.ErrNotLeader.Error()) {
			t.Errorf("Cordon on a follower's elector: %v", err)
		}
		// Sent without a call, it is carried out all the same.
		if err := c.Node(first).SendTo(t.Context(), elector(t, c, first), &leaderv1.Cordon{Node: follower}); err != nil {
			t.Fatal(err)
		}
		settle(time.Second)
		if got := cordoned(t, c, "a", "b", "c"); got != follower {
			t.Errorf("cordoned %q", got)
		}
	})
}
