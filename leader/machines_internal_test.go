package leader

import (
	"flag"
	"os"
	"slices"
	"testing"
)

var update = flag.Bool("update", false, "rewrite testdata/*.dot from the machines")

// The singleton's machine has no state it can get stuck in, and reaches
// every state from idle; its diagram is testdata/singleton.dot, which this
// test keeps current (go test -run Machines -update).
func TestMachines(t *testing.T) {
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
