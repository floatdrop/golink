package cli

import (
	"testing"

	"github.com/floatdrop/golink/tools/client"
)

func TestEventLine(t *testing.T) {
	got := eventLine(client.EventView{
		Time: "not a time", Kind: "exit", Missed: 3, Reason: "boom",
		Process: &client.ProcessView{PID: "<a.1.2>", Label: "order", Names: []string{"orders"}},
	})
	if got != `not a time exit <a.1.2> label=order name=orders reason="boom" missed=3` {
		t.Fatal(got)
	}
}
