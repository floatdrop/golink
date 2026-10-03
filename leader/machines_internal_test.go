package leader

import (
	"flag"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/floatdrop/fsm"
)

var update = flag.Bool("update", false, "rewrite testdata/*.dot and README.md's diagrams from the machines")

// No machine has a state it can get stuck in, and each reaches every state
// from its initial one.
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
	for name, m := range map[string]*fsm.Machine[standing]{"fixed view": fixedView, "open view": openView} {
		if got := m.Terminals(); len(got) != 0 {
			t.Errorf("%s: terminal standings %v: a peer would stay there for good", name, got)
		}
		if got := m.Unreachable(live); len(got) != 0 {
			t.Errorf("%s: unreachable standings %v", name, got)
		}
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
}

// The machines' diagrams are testdata/*.dot, and README.md draws each in a
// mermaid block after a "<!-- diagram: name -->" line; this keeps them all
// current (go test -run Diagrams -update).
func TestDiagrams(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(readme)
	for _, d := range []struct{ name, file, dot, mermaid string }{
		{"singleton", "singleton", lifecycle.DOT(), lifecycle.Mermaid()},
		{"election", "election", election.DOT(), election.Mermaid()},
		{"fixed view", "fixed-view", fixedView.DOT(), fixedView.Mermaid()},
		{"open view", "open-view", openView.DOT(), openView.Mermaid()},
	} {
		golden(t, "testdata/"+d.file+".dot", d.dot)

		marker := "<!-- diagram: " + d.name + " -->\n```mermaid\n"
		before, rest, ok := strings.Cut(text, marker)
		if !ok {
			t.Errorf("README.md has no %q", marker)
			continue
		}
		got, after, ok := strings.Cut(rest, "```\n")
		if !ok {
			t.Errorf("README.md's %s diagram is not closed", d.name)
			continue
		}
		if got == d.mermaid {
			continue
		}
		if *update {
			text = before + marker + d.mermaid + "```\n" + after
			continue
		}
		t.Errorf("README.md's %s diagram is not the machine's; run go test -run Diagrams -update\nthe machine's:\n%s", d.name, d.mermaid)
	}
	if *update && text != string(readme) {
		if err := os.WriteFile("README.md", []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
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
		t.Errorf("%s is not the machine's diagram; run go test -run Diagrams -update\ngot:\n%s", path, got)
	}
}
