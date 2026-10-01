package mcpserver_test

import (
	"strings"
	"testing"

	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
	"github.com/floatdrop/grpcproc/tools/mcpserver"
)

func TestGlobalNames(t *testing.T) {
	f := testcluster.Start(t, "nonames")
	cs := connect(t, f, mcpserver.Options{})
	var out struct {
		Names []client.NameView `json:"names"`
	}
	if msg := call(t, cs, "global_names", map[string]any{"prefix": "room:"}, &out); msg != "" || len(out.Names) != 1 || out.Names[0].PID != f.Talker.String() {
		t.Fatalf("list: %q %v", msg, out.Names)
	}
	if msg := call(t, cs, "global_names", map[string]any{"name": "room:talker"}, &out); msg != "" || len(out.Names) != 1 {
		t.Fatalf("lookup: %q %v", msg, out.Names)
	}
	if msg := call(t, cs, "global_names", map[string]any{"name": "nobody"}, &out); msg != "" || len(out.Names) != 0 {
		t.Fatalf("nobody: %q %v", msg, out.Names)
	}
	if msg := call(t, cs, "global_names", map[string]any{"node": "nonames"}, &out); !strings.Contains(msg, "no global names") {
		t.Fatalf("on nonames: %q", msg)
	}
}
