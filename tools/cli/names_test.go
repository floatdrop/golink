package cli_test

import (
	"strings"
	"testing"

	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
)

func TestNamesAndMetadata(t *testing.T) {
	f := testcluster.Start(t, "nonames")
	has(t, ok(t, run(t, f, "names")), "NAME PID", "room:talker "+f.Talker.String())
	has(t, ok(t, run(t, f, "names", "--prefix", "room:", "--limit", "1")), "room:talker")
	has(t, ok(t, run(t, f, "names", "room:talker")), "room:talker "+f.Talker.String())
	has(t, ok(t, run(t, f, "--json", "names", "room:talker")), `"pid": "`+f.Talker.String())
	if r := run(t, f, "names", "nobody"); r.code != 1 || !strings.Contains(r.stderr, `no process holds "nobody"`) {
		t.Fatalf("nobody: %+v", r)
	}
	if r := run(t, f, "names", "a", "b"); r.code != 2 {
		t.Fatalf("two names: %+v", r)
	}
	if r := run(t, f, "names", "--node", "nonames"); r.code != 1 || !strings.Contains(r.stderr, "no global names") {
		t.Fatalf("on nonames: %+v", r)
	}
	if r := run(t, f, "names", "--node", "nonames", "x"); r.code != 1 || !strings.Contains(r.stderr, "no global names") {
		t.Fatalf("lookup on nonames: %+v", r)
	}
	if r := run(t, f, "names", "--bogus"); r.code != 2 {
		t.Fatalf("bad flag: %+v", r)
	}

	has(t, ok(t, run(t, f, "node")), "metadata:      version=1.0")
	if out := ok(t, run(t, f, "node", "b")); strings.Contains(out, "metadata:") {
		t.Fatalf("b has no metadata:\n%s", out)
	}
	has(t, ok(t, run(t, f, "nodes")), "METADATA", "version=1.0")
	has(t, ok(t, run(t, f, "inspect", "talker")), "globals: room:talker")
}
