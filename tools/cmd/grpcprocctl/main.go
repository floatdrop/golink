// Command grpcprocctl inspects and operates grpcproc nodes through their
// Inspector, and serves the same as MCP tools to AI agents.
//
//	go install github.com/floatdrop/grpcproc/tools/cmd/grpcprocctl@latest
//	grpcprocctl --plaintext --addr localhost:9000 ps --sort mailbox
//	claude mcp add grpcproc -- grpcprocctl --plaintext --addr localhost:9000 mcp
package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/floatdrop/grpcproc/tools/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := cli.Main(ctx, os.Args[1:], cli.Env{})
	stop()
	os.Exit(code)
}
