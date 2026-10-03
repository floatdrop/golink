package leader

import (
	"flag"
	"os"
	"slices"
	"testing"

	"github.com/floatdrop/fsm"
)

var update = flag.Bool("update", false, "rewrite testdata/*.dot from the machines")

// No machine has a state it can get stuck in, and each reaches every state
// from its initial one. Their diagrams are testdata/*.dot, which this test
// keeps current (go test -run Machines -update).
func TestMachines(t *testing.T) {
	if got := election.Terminals(); len(got) != 0 {
		t.Errorf("terminal stances %v: an elector would stay there for good", got)
	}
	if got := election.Unreachable(follower); len(got) != 0 {
		t.Errorf("unreachable stances %v", got)
	}
	if got, want := election.States(), []stance{follower, preCandidate, candidate, leading, handingOver}; !slices.Equal(got, want) {
		t.Errorf("stances %v, want %v", got, want)
	}
	golden(t, "testdata/election.dot", election.DOT())
	for name, m := range map[string]*fsm.Machine[standing]{"fixed-view": fixedView, "open-view": openView} {
		if got := m.Terminals(); len(got) != 0 {
			t.Errorf("%s: terminal standings %v: a peer would stay there for good", name, got)
		}
		if got := m.Unreachable(live); len(got) != 0 {
			t.Errorf("%s: unreachable standings %v", name, got)
		}
		golden(t, "testdata/"+name+".dot", m.DOT())
	}

	if got := lifecycle.Terminals(); len(got) != 0 {
		t.Errorf("terminal phases %v: a singleton would stay there for good", got)
	}
	if got := lifecycle.Unreachable(idle); len(got) != 0 {
		t.Errorf("unreachable phases %v", got)
	}
	if got, want := lifecycle.States(), []phase{idle, starting, running, stopping}; !slices.Equal(got, want) {
		t.Errorf("phases %v, want %v", got, want)
	}
	golden(t, "testdata/singleton.dot", lifecycle.DOT())
}

func golden(t *testing.T, path, got string) {
	t.Helper()
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Errorf("%s is not the machine's diagram; run go test -run Machines -update\ngot:\n%s", path, got)
	}
}
