// Package shared holds what every framework's benchmarks use, so each can
// live in its own package: frameworks that register the same protobuf file
// names cannot share a test binary.
package shared

import (
	"net"
	"strconv"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Msg is the message every benchmark sends: a protobuf, because Hollywood
// and Proto.Actor need one to go remote.
type Msg = wrapperspb.Int64Value

// FreeAddr returns a loopback address with a free port, for frameworks that
// cannot listen on port 0 and report the port.
func FreeAddr(b *testing.B) string {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

// FreePort is FreeAddr's port, for frameworks configured with host and port apart.
func FreePort(b *testing.B) int {
	b.Helper()
	_, port, _ := net.SplitHostPort(FreeAddr(b))
	n, err := strconv.Atoi(port)
	if err != nil {
		b.Fatal(err)
	}
	return n
}
