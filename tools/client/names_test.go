package client_test

import (
	"errors"
	"testing"

	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
)

func TestNames(t *testing.T) {
	f := testcluster.Start(t, "nonames", "unlisted")
	c := client.New(f.C.Conn("a"))
	n, ok, err := c.LookupName(t.Context(), "", "room:talker")
	if err != nil || !ok || n.PID != f.Talker.String() || n.Name != "room:talker" {
		t.Fatalf("lookup: %+v %v %v", n, ok, err)
	}
	if _, ok, err := c.LookupName(t.Context(), "b", "nobody"); err != nil || ok {
		t.Fatalf("lookup of nobody on b: %v %v", ok, err)
	}
	names, err := c.Names(t.Context(), "", "room:", 1)
	if err != nil || len(names) != 1 || names[0].PID != f.Talker.String() {
		t.Fatalf("names: %v %v", names, err)
	}
	if _, _, err := c.LookupName(t.Context(), "nonames", "x"); !errors.Is(err, client.ErrNoNames) {
		t.Fatalf("lookup on nonames: %v", err)
	}
	if _, err := c.Names(t.Context(), "nonames", "", 0); !errors.Is(err, client.ErrNoNames) {
		t.Fatalf("names on nonames: %v", err)
	}
	if _, err := c.Names(t.Context(), "unlisted", "", 0); !errors.Is(err, client.ErrUnlisted) {
		t.Fatalf("names on unlisted: %v", err)
	}
	if _, err := c.Names(t.Context(), "nowhere", "", 0); err == nil || errors.Is(err, client.ErrNoNames) {
		t.Fatalf("names on a node that does not exist: %v", err)
	}
	n0, err := c.Node(t.Context(), "")
	if err != nil || n0.Metadata["version"] != "1.0" {
		t.Fatalf("metadata: %v %v", n0.Metadata, err)
	}
	p, err := c.Process(t.Context(), "", "talker", false, 0)
	if err != nil || len(p.Globals) != 1 || p.Globals[0] != "room:talker" {
		t.Fatalf("globals: %v %v", p.Globals, err)
	}
}
