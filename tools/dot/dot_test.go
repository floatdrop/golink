package dot_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/floatdrop/golink/tools/client"
	"github.com/floatdrop/golink/tools/dot"
)

func TestRender(t *testing.T) {
	var b strings.Builder
	err := dot.Render(&b, []dot.Node{
		{Name: "a", Processes: []client.ProcessView{
			{PID: "<a.1.1>", Names: []string{"sup"}, Label: "supervisor"},
			{PID: "<a.1.2>", Names: []string{"orders"}, Label: "order", Parent: "<a.1.1>", Mailbox: 3, OldestWait: "2s"},
			{PID: "<a.1.3>", Label: `say "hi"`, Parent: "<gone.1.1>"},
		}},
		{Name: "b", Processes: []client.ProcessView{
			{PID: "<b.1.1>", Label: "worker", Parent: "<a.1.1>"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `digraph golink {
	rankdir=LR;
	node [shape=box, fontname="monospace", fontsize=10];
	subgraph cluster_0 {
		label="a";
		"<a.1.1>" [label="sup\n<a.1.1>\nsupervisor", style="rounded,bold"];
		"<a.1.2>" [label="orders\n<a.1.2>\norder\nmailbox 3, oldest 2s", color="red"];
		"<a.1.3>" [label="say \"hi\"\n<a.1.3>"];
	}
	subgraph cluster_1 {
		label="b";
		"<b.1.1>" [label="worker\n<b.1.1>"];
	}
	"<a.1.1>" -> "<a.1.2>";
	"<a.1.1>" -> "<b.1.1>";
}
`
	if b.String() != want {
		t.Fatalf("got:\n%s\nwant:\n%s", b.String(), want)
	}
}

type failing struct{}

func (failing) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestRenderWriteError(t *testing.T) {
	if err := dot.Render(failing{}, nil); err == nil {
		t.Fatal("write error lost")
	}
}
