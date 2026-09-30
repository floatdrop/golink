package client_test

import (
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
)

func TestElection(t *testing.T) {
	f := testcluster.Start(t, "c")
	f.Elect(t, "sched", "a", "b", "c")
	c := client.New(f.C.Conn("a"))
	ctx := t.Context()

	views, err := c.Election(ctx, "sched")
	if err != nil || len(views) != 3 {
		t.Fatalf("%+v %v", views, err)
	}
	lead := client.Leading(views)
	for _, v := range views {
		if v.Leader != lead || v.Term == 0 || v.Quorum != 2 || !slices.Equal(v.View, []string{"a", "b", "c"}) || v.State == "" {
			t.Errorf("%+v, leader %s", v, lead)
		}
		if (v.Node == lead) != (v.Role == "leader") || (v.Node == lead) != strings.HasPrefix(v.Singleton, "<") {
			t.Errorf("%+v, leader %s", v, lead)
		}
	}
	if _, err := c.Election(ctx, "nothing"); err == nil || !strings.Contains(err.Error(), `no node runs an election called "nothing"`) {
		t.Errorf("an election nobody runs: %v", err)
	}

	// Moved to a node named, then to whichever follower is most recent.
	next := "b"
	if lead == "b" {
		next = "c"
	}
	was, err := c.MoveLeader(ctx, "sched", next)
	if err != nil || was != lead {
		t.Fatalf("moved from %s: %v", was, err)
	}
	f.Leading(t, "sched", "a", func(l string) bool { return l == next })
	if was, err := c.MoveLeader(ctx, "sched", ""); err != nil || was != next {
		t.Fatalf("moved from %s: %v", was, err)
	}
	f.Leading(t, "sched", "a", func(l string) bool { return l != next })
	if _, err := c.MoveLeader(ctx, "sched", "zzz"); err == nil || !strings.Contains(err.Error(), "zzz is not in the view") {
		t.Errorf("moved to a stranger: %v", err)
	}

	// Cordoned, and let go.
	if _, err := c.Cordon(ctx, "sched", "b", false); err != nil {
		t.Fatal(err)
	}
	if views, _ := c.Election(ctx, "sched"); !slices.Equal(views[0].Cordoned, []string{"b"}) {
		t.Errorf("%+v", views[0])
	}
	if _, err := c.Cordon(ctx, "sched", "b", true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Cordon(ctx, "sched", "\xff", false); err == nil {
		t.Error("a node name that is not UTF-8 went out")
	}

	// Every election is found by its electors' names, whatever the others
	// are called.
	f.Elect(t, "jobs", "a", "b")
	if _, err := f.C.Node("a").Spawn(func(p *grpcproc.Process[proto.Message]) error {
		_, err := p.Receive()
		return err
	}, grpcproc.WithName("myleader/decoy")); err != nil {
		t.Fatal(err)
	}
	all, err := c.Elections(ctx)
	if err != nil || len(all) != 2 || all[0].Cluster != "jobs" || all[1].Cluster != "sched" ||
		len(all[0].Electors) != 2 || len(all[1].Electors) != 3 || all[0].Leading == "" {
		t.Fatalf("%+v %v", all, err)
	}
}
