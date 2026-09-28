package cli_test

import (
	"encoding/json/v2"
	"slices"
	"strings"
	"testing"

	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
)

// leaders reads the LEADER column of a leader table.
func leaders(t *testing.T, table string) []string {
	t.Helper()
	var out []string
	for i, line := range strings.Split(strings.TrimSpace(table), "\n") {
		if i > 0 {
			out = append(out, strings.Fields(line)[3])
		}
	}
	return out
}

func TestLeader(t *testing.T) {
	f := testcluster.Start(t, "c")
	f.Elect(t, "sched", "a", "b", "c")
	lead := f.Leading(t, "sched", "a", func(string) bool { return true })

	table := ok(t, run(t, f, "leader", "sched"))
	has(t, table, "NODE ROLE TERM LEADER VIEW STATE CORDONED UNREACHABLE SINGLETON BACKOFF ERROR", "a,b,c")
	if got := leaders(t, table); !slices.Equal(got, []string{lead, lead, lead}) {
		t.Fatalf("leaders %v, want %s", got, lead)
	}
	has(t, ok(t, run(t, f, "leader", "status", "sched")), "leader", "follower")
	var views []client.ElectorView
	if err := json.Unmarshal([]byte(ok(t, run(t, f, "--json", "leader", "sched"))), &views); err != nil || len(views) != 3 {
		t.Fatalf("%+v %v", views, err)
	}

	// Moved to a node named: the table shows it leading, once all agree.
	next := "b"
	if lead == "b" {
		next = "c"
	}
	if got := leaders(t, ok(t, run(t, f, "leader", "move", "--to", next, "sched"))); !slices.Equal(got, []string{next, next, next}) {
		t.Fatalf("after moving to %s: %v", next, got)
	}
	if got := leaders(t, ok(t, run(t, f, "leader", "move", "sched"))); got[0] == next || len(slices.Compact(got)) != 1 {
		t.Fatalf("after moving on from %s: %v", next, got)
	}

	// Cordoned: every node lists it, and it does not lead.
	lead = f.Leading(t, "sched", "a", func(string) bool { return true })
	table = ok(t, run(t, f, "leader", "cordon", "sched", lead))
	for _, l := range leaders(t, table) {
		if l == lead {
			t.Fatalf("%s leads, cordoned:\n%s", lead, table)
		}
	}
	has(t, table, " "+lead+" ")
	views = nil
	if err := json.Unmarshal([]byte(ok(t, run(t, f, "--json", "leader", "uncordon", "sched", lead))), &views); err != nil {
		t.Fatal(err)
	}
	for _, v := range views {
		if len(v.Cordoned) > 0 {
			t.Errorf("still cordoned on %s: %v", v.Node, v.Cordoned)
		}
	}
}

func TestLeaderErrors(t *testing.T) {
	f := testcluster.Start(t, "c")
	f.Elect(t, "sched", "a", "b", "c")
	for _, args := range [][]string{
		{"leader"},
		{"leader", "cordon", "sched"},
		{"leader", "status", "sched", "extra"},
		{"leader", "move", "--bogus", "sched"},
	} {
		if r := run(t, f, args...); r.code != 2 {
			t.Errorf("%v: exit %d, %s", args, r.code, r.stderr)
		}
	}
	for args, want := range map[string]string{
		"leader nothing":             `no node runs an election called "nothing"`,
		"leader move --to zzz sched": "zzz is not in the view",
		"leader cordon nothing b":    `no node runs an election called "nothing"`,
		"leader uncordon sched \xff": "invalid UTF-8",
	} {
		r := run(t, f, strings.Fields(args)...)
		if r.code != 1 || !strings.Contains(r.stderr, want) {
			t.Errorf("%s: exit %d, %q, want %q", args, r.code, r.stderr, want)
		}
	}
}

// A change the leader made, which a node cut off from the one grpcprocctl
// asks cannot be asked about: the command says the nodes do not all agree,
// and shows how things stand.
func TestLeaderChangeNotEveryoneHeard(t *testing.T) {
	f := testcluster.Start(t, "c")
	f.Elect(t, "sched", "a", "b", "c")
	lead := f.Leading(t, "sched", "a", func(string) bool { return true })
	cut := "c" // neither a, which grpcprocctl asks, nor the leader
	if lead == "c" {
		cut = "b"
	}
	f.C.Partition("a", cut)
	r := run(t, f, "--timeout", "1s", "leader", "cordon", "sched", cut)
	if r.code != 0 || !strings.Contains(r.stderr, "the nodes do not all agree yet") {
		t.Fatalf("exit %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	has(t, r.stdout, "NODE", cut)
}
