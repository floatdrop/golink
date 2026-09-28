package leader_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/grpcproctest"
	"github.com/floatdrop/grpcproc/leader"
)

// stored is spec with a File for each node, in dir.
func stored(dir string) func(j *journal, node string) leader.Spec[counter] {
	return func(j *journal, node string) leader.Spec[counter] {
		s := spec(j)
		s.Store = leader.File(filepath.Join(dir, node))
		return s
	}
}

// restartAll kills every node of nodes at once, then starts each again,
// with its elector as s describes it.
func restartAll(t *testing.T, c *grpcproctest.Cluster, j *journal, s func(j *journal, node string) leader.Spec[counter], nodes ...string) {
	t.Helper()
	for _, n := range nodes {
		c.Kill(n)
	}
	for _, n := range nodes {
		c.Restart(n)
		if _, err := leader.Start(c.Node(n), s(j, n)); err != nil {
			t.Fatal(err)
		}
	}
}

// lastStart is what node's singleton started from last, as the journal has
// it.
func lastStart(j *journal, node string) string {
	for _, e := range slices.Backward(strings.Split(j.String(), "; ")) {
		if strings.HasPrefix(e, node+" starts from ") {
			return strings.TrimPrefix(e, node+" ")
		}
	}
	return ""
}

// With a Store on every node, a cluster that stops whole starts again from
// its last checkpoint, cordons included, and in a later term. Without, it
// starts from nothing.
func TestFullRestart(t *testing.T) {
	all := []string{"a", "b", "c"}
	for _, keep := range []bool{true, false} {
		t.Run(fmt.Sprint("store ", keep), func(t *testing.T) {
			s := stored(t.TempDir())
			if !keep {
				s = func(j *journal, _ string) leader.Spec[counter] { return spec(j) }
			}
			nodes(t, all, all, s, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
				settle(2 * time.Second)
				first, term := elected(t, c, all...)
				count(t, c, first, 7)
				out := others(first)[0]
				if err := leader.Cordon(t.Context(), c.Node(first), "test", out); err != nil {
					t.Fatal(err)
				}
				restartAll(t, c, j, s, all...)
				settle(2 * time.Second)
				second, term2 := elected(t, c, all...)
				want, cordon := "starts from 0", ""
				if keep {
					want, cordon = "starts from 7", out
					if term2 <= term || second == out {
						t.Errorf("%s leads in term %d, after term %d, with %s cordoned", second, term2, term, out)
					}
				}
				if got := lastStart(j, second); got != want {
					t.Errorf("%s %s, want %s", second, got, want)
				}
				if got := cordoned(t, c, all...); got != cordon {
					t.Errorf("cordoned %q, want %q", got, cordon)
				}
			})
		})
	}
}

// A node alone, with a Store, starts again from its last checkpoint, and
// its terms go on growing: its Lease.Term stays a fencing token.
func TestRestartAlone(t *testing.T) {
	dir := t.TempDir()
	alone := func(j *journal, node string) leader.Spec[counter] {
		s := stored(dir)(j, node)
		s.Voters = []string{"a"}
		return s
	}
	nodes(t, []string{"a"}, []string{"a"}, alone, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(time.Second)
		count(t, c, "a", 5)
		term := j.lease("a").Term()
		restartAll(t, c, j, alone, "a")
		settle(time.Second)
		if got := lastStart(j, "a"); got != "starts from 5" {
			t.Errorf("a %s", got)
		}
		if term2 := j.lease("a").Term(); term2 <= term {
			t.Errorf("term %d after a restart, was %d", term2, term)
		}
	})
}

// A voter that restarts remembers whom it voted for, and in which term.
func TestRestartedVoterKeepsItsVote(t *testing.T) {
	all := []string{"a", "b", "c"}
	s := stored(t.TempDir())
	nodes(t, all, all, s, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(2 * time.Second)
		lead, term := elected(t, c, all...)
		// The leader won a majority: one follower at least voted for it.
		i := slices.IndexFunc(others(lead), func(n string) bool { return inspect(t, c, n)["voted_for"] == lead })
		if i < 0 {
			t.Fatal("nobody voted for the leader")
		}
		voter := others(lead)[i]
		restartAll(t, c, j, s, voter)
		got := inspect(t, c, voter)
		if got["voted_for"] != lead || got["term"] != fmt.Sprint(term) {
			t.Errorf("restarted, %s voted for %q in term %s; it voted for %s in %d", voter, got["voted_for"], got["term"], lead, term)
		}
	})
}

// memory is a Store in memory, whose Saves fail as fails says: the nth
// fails if fails[n], counting from 1.
type memory struct {
	mu    sync.Mutex
	b     []byte
	saves int
	fails map[int]bool
}

func (m *memory) Load() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.b, nil
}

func (m *memory) Save(b []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saves++
	if m.fails[m.saves] {
		return errors.New("disk full")
	}
	m.b = b
	return nil
}

// An elector whose Save fails exits, before anything that tells of what it
// did not save leaves it, and its supervisor starts it again from what the
// Store holds.
func TestSaveThatFails(t *testing.T) {
	for name, voters := range map[string][]string{
		"alone":      {"a"},
		"in a group": {"a", "b", "c"},
	} {
		t.Run(name, func(t *testing.T) {
			failing := func(j *journal, node string) leader.Spec[counter] {
				s := spec(j)
				s.Voters = voters
				if node == "a" {
					s.Store = &memory{fails: map[int]bool{1: true}}
				}
				return s
			}
			nodes(t, voters, voters, failing, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
				was := elector(t, c, "a")
				settle(3 * time.Second)
				if elector(t, c, "a") == was {
					t.Error("the elector of a did not restart")
				}
				elected(t, c, voters...)
			})
		})
	}
}

// A checkpoint the leader cannot save fails, and the singleton starts again
// from the last one saved.
func TestCheckpointThatCannotBeSaved(t *testing.T) {
	failing := func(j *journal, _ string) leader.Spec[counter] {
		s := spec(j)
		s.Voters = []string{"a"}
		// The first Save is the term's, the second the first checkpoint's.
		s.Store = &memory{fails: map[int]bool{3: true}}
		return s
	}
	nodes(t, []string{"a"}, []string{"a"}, failing, func(t *testing.T, c *grpcproctest.Cluster, j *journal) {
		settle(time.Second)
		count(t, c, "a", 3) // saved
		if _, err := grpcproc.Named[counter]("a", "singleton").Call[counter](t.Context(), c.Node("a"), wrapperspb.Int64(4)); err == nil {
			t.Error("a checkpoint that was not saved succeeded")
		}
		settle(time.Second)
		if got := lastStart(j, "a"); got != "starts from 3" {
			t.Errorf("a %s", got)
		}
	})
}

// A Store that cannot be loaded keeps the elector from starting.
func TestStoreThatCannotBeLoaded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a")
	if err := os.WriteFile(path, []byte("not a protobuf"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := spec(&journal{})
	s.Voters, s.Store = []string{"a"}, leader.File(path)
	synctest.Test(t, func(t *testing.T) {
		c := grpcproctest.New(t, "a")
		sup, err := leader.Start(c.Node("a"), s)
		if err != nil {
			t.Fatal(err)
		}
		settle(time.Second)
		if _, ok := c.Node("a").Process(sup); ok {
			t.Error("an election whose Store cannot be loaded still runs")
		}
	})
}

func TestFile(t *testing.T) {
	dir := t.TempDir()
	f := leader.File(filepath.Join(dir, "state"))
	if b, err := f.Load(); b != nil || err != nil {
		t.Errorf("Load before any Save: %q, %v", b, err)
	}
	for _, s := range []string{"one", "two"} {
		if err := f.Save([]byte(s)); err != nil {
			t.Fatal(err)
		}
		if b, err := f.Load(); string(b) != s || err != nil {
			t.Errorf("Load after saving %q: %q, %v", s, b, err)
		}
	}

	if err := leader.File(filepath.Join(dir, "missing", "state")).Save(nil); err == nil {
		t.Error("Save in a directory that does not exist")
	}
	// A directory where the file should be: it cannot be loaded, or renamed
	// over, and the new file does not stay behind.
	sub := filepath.Join(dir, "sub")
	if err := os.MkdirAll(filepath.Join(sub, "inside"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.File(sub).Load(); err == nil {
		t.Error("Load of a directory")
	}
	if err := leader.File(sub).Save([]byte("x")); err == nil {
		t.Error("Save over a directory")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if !slices.Equal(names, []string{"state", "sub"}) {
		t.Errorf("left in the directory: %v", names)
	}
}
