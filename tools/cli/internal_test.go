package cli

import (
	"testing"

	"github.com/floatdrop/grpcproc/tools/client"
)

func TestEventLine(t *testing.T) {
	got := eventLine(client.EventView{
		Time: "not a time", Kind: "exit", Missed: 3, Reason: "boom",
		Process: &client.ProcessView{PID: "<a.1.2>", Label: "order", Name: "orders"},
	})
	if got != `not a time exit <a.1.2> label=order name=orders reason="boom" missed=3` {
		t.Fatal(got)
	}
}

func TestLoopback(t *testing.T) {
	for addr, want := range map[string]bool{
		"localhost:9911": true, "127.0.0.1:9911": true, "[::1]:9911": true,
		":9911": false, "0.0.0.0:9911": false, "10.0.0.5:9911": false, "observer.internal:80": false, "nonsense": false,
	} {
		if got := loopback(addr); got != want {
			t.Errorf("%s: %v", addr, got)
		}
	}
}
