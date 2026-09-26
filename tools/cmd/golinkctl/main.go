// Command golinkctl inspects and operates golink nodes through their
// Inspector, and serves the same as MCP tools to AI agents.
//
//	go install github.com/floatdrop/golink/tools/cmd/golinkctl@latest
//	golinkctl --plaintext --addr localhost:9000 ps --sort mailbox
//	claude mcp add golink -- golinkctl --plaintext --addr localhost:9000 mcp
package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/floatdrop/golink/tools/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := cli.Main(ctx, os.Args[1:], cli.Env{})
	stop()
	os.Exit(code)
}
