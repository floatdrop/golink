package cli_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json/v2"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/inspect"
	"github.com/floatdrop/grpcproc/tools/cli"
	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
)

type result struct {
	code           int
	stdout, stderr string
}

// run runs grpcprocctl against the fixture's node a.
func run(t *testing.T, f *testcluster.Fixture, args ...string) result {
	t.Helper()
	var out, errOut bytes.Buffer
	code := cli.Main(t.Context(), args, cli.Env{
		Stdout: &out, Stderr: &errOut,
		Dial: func(context.Context, cli.Conn) (grpc.ClientConnInterface, func() error, error) {
			return f.C.Conn("a"), func() error { return nil }, nil
		},
		Getenv: func(string) string { return "" },
	})
	return result{code, out.String(), errOut.String()}
}

func ok(t *testing.T, r result) string {
	t.Helper()
	if r.code != 0 {
		t.Fatalf("exit %d\nstdout: %s\nstderr: %s", r.code, r.stdout, r.stderr)
	}
	return r.stdout
}

// has checks s contains parts, with runs of spaces counted as one, so
// tabwriter's column widths do not matter.
func has(t *testing.T, s string, parts ...string) {
	t.Helper()
	flat := strings.Join(strings.Fields(s), " ")
	for _, p := range parts {
		if !strings.Contains(flat, strings.Join(strings.Fields(p), " ")) {
			t.Fatalf("missing %q in:\n%s", p, s)
		}
	}
}

func TestReadCommands(t *testing.T) {
	f := testcluster.Start(t)
	has(t, ok(t, run(t, f, "node")), "node:          a#", "PEER", "b#", "out", "in", "up")
	has(t, ok(t, run(t, f, "node", "b")), "node:          b#")
	has(t, ok(t, run(t, f, "nodes")), "NODE", "PEERS", "a ", "b ")
	has(t, ok(t, run(t, f, "ps")), "PID", "sup", "w1", "stuck", "talker", "supervisor")
	out := ok(t, run(t, f, "ps", "--sort", "mailbox", "--limit", "1"))
	has(t, out, "stuck")
	if strings.Count(out, "\n") != 2 {
		t.Fatalf("limit ignored:\n%s", out)
	}
	has(t, ok(t, run(t, f, "ps", "--node", "b", "--name", "ech", "--state", "idle")), "echo")
	has(t, ok(t, run(t, f, "inspect", "talker")), "names:", "talker", "state:          ready")
	has(t, ok(t, run(t, f, "inspect", "--wait", "10ms", "stuck")), "inspect:", "busy", "mailbox:", "(peak")
	has(t, ok(t, run(t, f, "inspect", f.Echo.String())), "echo")
	has(t, ok(t, run(t, f, "dot")), "digraph grpcproc", `label="a"`, "rounded,bold", "->")
	has(t, ok(t, run(t, f, "dot", "--cluster")), `label="a"`, `label="b"`, "echo")
}

func TestJSON(t *testing.T) {
	f := testcluster.Start(t)
	var n client.NodeView
	if err := json.Unmarshal([]byte(ok(t, run(t, f, "--json", "node"))), &n); err != nil || n.Name != "a" {
		t.Fatalf("%+v %v", n, err)
	}
	var nodes []client.NodeView
	if err := json.Unmarshal([]byte(ok(t, run(t, f, "--json", "nodes"))), &nodes); err != nil || len(nodes) != 2 {
		t.Fatalf("%+v %v", nodes, err)
	}
	var ps []client.ProcessView
	if err := json.Unmarshal([]byte(ok(t, run(t, f, "--json", "ps", "--min-mailbox", "1"))), &ps); err != nil || len(ps) != 1 || ps[0].Mailbox != 3 {
		t.Fatalf("%+v %v", ps, err)
	}
	var p client.ProcessView
	if err := json.Unmarshal([]byte(ok(t, run(t, f, "--json", "inspect", "talker"))), &p); err != nil || p.Inspect["state"] != "ready" {
		t.Fatalf("%+v %v", p, err)
	}
}

func TestWatch(t *testing.T) {
	f := testcluster.Start(t)
	done := make(chan result, 2)
	go func() { done <- run(t, f, "watch", "--kind", "exit", "--count", "1") }()
	go func() { done <- run(t, f, "--json", "watch", "--kind", "spawn", "--count", "1") }()
	var got []result
	deadline := time.After(5 * time.Second)
	for len(got) < 2 {
		_, _ = grpcproc.Spawn(f.C.Node("a"), func(*grpcproc.Process[proto.Message]) error { return nil }, grpcproc.WithName("brief"))
		select {
		case r := <-done:
			got = append(got, r)
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatal("watch did not finish")
		}
	}
	all := ok(t, got[0]) + ok(t, got[1])
	has(t, all, " exit <a.", "name=brief", `reason="normal"`, `"kind":"spawn"`)
}

func TestWrites(t *testing.T) {
	f := testcluster.Start(t)
	ok(t, run(t, f, "loglevel", "talker", "debug"))
	has(t, ok(t, run(t, f, "inspect", "talker")), "log level:      DEBUG")
	ok(t, run(t, f, "exit", "talker", "bye"))
	time.Sleep(20 * time.Millisecond)
	if r := run(t, f, "inspect", "talker"); r.code != 1 || !strings.Contains(r.stderr, "no process") {
		t.Fatalf("%+v", r)
	}
}

func TestUsageAndErrors(t *testing.T) {
	f := testcluster.Start(t)
	cases := []struct {
		args []string
		code int
		msg  string
	}{
		{nil, 2, "Usage:"},
		{[]string{"--bogus"}, 2, "flag provided but not defined"},
		{[]string{"frobnicate"}, 2, "unknown command"},
		{[]string{"ps", "--bogus"}, 2, ""},
		{[]string{"ps", "--sort", "age"}, 2, "bad sort"},
		{[]string{"ps", "--state", "sleeping"}, 1, "bad state"},
		{[]string{"inspect"}, 2, "want one process"},
		{[]string{"inspect", "nobody"}, 1, "no process"},
		{[]string{"exit"}, 2, "want a process"},
		{[]string{"loglevel", "talker"}, 2, "want a process and a level"},
		{[]string{"loglevel", "talker", "loud"}, 2, "bad level"},
		{[]string{"node", "nowhere"}, 1, "node nowhere:"},
		{[]string{"nodes", "--bogus"}, 2, ""},
		{[]string{"node", "--bogus"}, 2, ""},
		{[]string{"inspect", "--bogus"}, 2, ""},
		{[]string{"watch", "--bogus"}, 2, ""},
		{[]string{"exit", "--bogus"}, 2, ""},
		{[]string{"loglevel", "--bogus"}, 2, ""},
		{[]string{"dot", "--bogus"}, 2, ""},
		{[]string{"mcp", "--bogus"}, 2, ""},
		{[]string{"dot", "--node", "nowhere"}, 1, "node nowhere:"},
		{[]string{"watch", "--node", "nowhere"}, 1, "node nowhere:"},
		{[]string{"exit", "nobody"}, 0, ""}, // exiting what is not there is not an error, as in Erlang
	}
	for _, tc := range cases {
		r := run(t, f, tc.args...)
		if r.code != tc.code || !strings.Contains(r.stderr, tc.msg) {
			t.Errorf("%v: exit %d, stderr %q", tc.args, r.code, r.stderr)
		}
	}
}

func TestDialFailure(t *testing.T) {
	var errOut bytes.Buffer
	code := cli.Main(t.Context(), []string{"--cacert", "/does/not/exist", "node"}, cli.Env{Stderr: &errOut, Getenv: func(string) string { return "" }})
	if code != 1 || !strings.Contains(errOut.String(), "does/not/exist") {
		t.Fatalf("%d %s", code, errOut.String())
	}
}

// A real node over TCP, reached with grpcprocctl's own dialing and $GRPCPROC_ADDR.
func TestRealDial(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	n, _ := grpcproc.NewNode(grpcproc.Config{Name: "solo", Resolver: grpcproc.StaticResolver{}})
	n.Register(srv)
	inspect.New(n).Register(srv)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = n.Stop(context.Background()); srv.Stop() })
	var out bytes.Buffer
	code := cli.Main(t.Context(), []string{"--plaintext", "node"}, cli.Env{Stdout: &out, Getenv: func(k string) string {
		if k == "GRPCPROC_ADDR" {
			return ln.Addr().String()
		}
		return ""
	}})
	if code != 0 || !strings.Contains(out.String(), "node:          solo#") {
		t.Fatalf("%d %s", code, out.String())
	}
}

func TestTLSCredentials(t *testing.T) {
	dir := t.TempDir()
	cert, key := writeCert(t, dir)
	bad := filepath.Join(dir, "bad.pem")
	_ = os.WriteFile(bad, []byte("not a certificate"), 0o600)
	cases := []struct {
		args []string
		fail string
	}{
		{[]string{"--cacert", cert, "--cert", cert, "--key", key, "--servername", "localhost"}, ""},
		{[]string{}, ""},
		{[]string{"--cacert", bad}, "no certificates"},
		{[]string{"--cert", cert}, "--cert and --key go together"},
	}
	for _, tc := range cases {
		var errOut bytes.Buffer
		// A dial that succeeds is followed by a request that cannot reach
		// anything; only credential errors say something else.
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		code := cli.Main(ctx, append(append([]string{"--addr", "127.0.0.1:1", "--timeout", "100ms"}, tc.args...), "node"), cli.Env{Stderr: &errOut, Getenv: func(string) string { return "" }})
		cancel()
		if code != 1 || (tc.fail != "" && !strings.Contains(errOut.String(), tc.fail)) || (tc.fail == "" && strings.Contains(errOut.String(), "grpcprocctl: ")) {
			t.Errorf("%v: %d %s", tc.args, code, errOut.String())
		}
	}
}

func writeCert(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()
	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(priv)
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	_ = os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	_ = os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600)
	return certFile, keyFile
}

func TestMCPCommand(t *testing.T) {
	f := testcluster.Start(t)
	serverT, clientT := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan int, 1)
	go func() {
		done <- cli.Main(ctx, []string{"mcp", "--allow-writes"}, cli.Env{
			Dial: func(context.Context, cli.Conn) (grpc.ClientConnInterface, func() error, error) {
				return f.C.Conn("a"), func() error { return nil }, nil
			},
			MCPTransport: serverT, Getenv: func(string) string { return "" },
			BuildInfo: stamped("v9.9.9"),
		})
	}()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test"}, nil).Connect(t.Context(), clientT, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v := session.InitializeResult().ServerInfo.Version; v != "v9.9.9" {
		t.Fatalf("mcp reports version %q", v)
	}
	tools, err := session.ListTools(t.Context(), nil)
	if err != nil || len(tools.Tools) != 7 {
		t.Fatalf("%v %v", tools, err)
	}
	_ = session.Close()
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("interrupting mcp exited %d", code)
	}
}

// runFailing is run with the given Inspector methods failing.
func runFailing(t *testing.T, f *testcluster.Fixture, failing []string, args ...string) result {
	t.Helper()
	var out, errOut bytes.Buffer
	fail := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		for _, m := range failing {
			if strings.HasSuffix(method, "/"+m) {
				return status.Error(codes.Unavailable, m+" failed")
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	code := cli.Main(t.Context(), args, cli.Env{
		Stdout: &out, Stderr: &errOut,
		Dial: func(context.Context, cli.Conn) (grpc.ClientConnInterface, func() error, error) {
			cc, err := grpc.NewClient("passthrough:///a", append(f.C.DialOptions(), grpc.WithUnaryInterceptor(fail))...)
			if err != nil {
				return nil, nil, err
			}
			return cc, cc.Close, nil
		},
		MCPTransport: badTransport{},
		Getenv:       func(string) string { return "" },
	})
	return result{code, out.String(), errOut.String()}
}

type badTransport struct{}

func (badTransport) Connect(context.Context) (mcp.Connection, error) {
	return nil, errors.New("no stdio here")
}

func TestFailingRequests(t *testing.T) {
	f := testcluster.Start(t)
	cases := []struct {
		failing []string
		args    []string
		msg     string
	}{
		{[]string{"GetNode"}, []string{"nodes"}, "GetNode failed"},
		{[]string{"GetNode"}, []string{"dot", "--cluster"}, "GetNode failed"},
		{[]string{"GetNode"}, []string{"dot"}, "GetNode failed"},
		{[]string{"ListProcesses"}, []string{"dot"}, "ListProcesses failed"},
		{nil, []string{"mcp"}, "no stdio here"},
	}
	for _, tc := range cases {
		if r := runFailing(t, f, tc.failing, tc.args...); r.code != 1 || !strings.Contains(r.stderr, tc.msg) {
			t.Errorf("%v: %d %q", tc.args, r.code, r.stderr)
		}
	}
}

func TestDialErrors(t *testing.T) {
	dir := t.TempDir()
	junk := filepath.Join(dir, "junk.pem")
	_ = os.WriteFile(junk, []byte("junk"), 0o600)
	for _, args := range [][]string{
		{"--plaintext", "--addr", "\x7f://bad", "node"}, // grpc refuses the target
		{"--cert", junk, "--key", junk, "node"},         // not a key pair
	} {
		var errOut bytes.Buffer
		if code := cli.Main(t.Context(), args, cli.Env{Stderr: &errOut}); code != 1 || !strings.HasPrefix(errOut.String(), "grpcprocctl: ") {
			t.Errorf("%q: %d %s", args, code, errOut.String())
		}
	}
}

// stamped is build info as `go install …@version` leaves it.
func stamped(v string) func() (*debug.BuildInfo, bool) {
	return func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{Main: debug.Module{Path: "github.com/floatdrop/grpcproc/tools", Version: v}}, true
	}
}

func TestVersion(t *testing.T) {
	versionOf := func(read func() (*debug.BuildInfo, bool)) string {
		t.Helper()
		var out bytes.Buffer
		// --version answers before anything is dialed: this Dial would fail.
		code := cli.Main(t.Context(), []string{"--version"}, cli.Env{Stdout: &out, BuildInfo: read,
			Dial: func(context.Context, cli.Conn) (grpc.ClientConnInterface, func() error, error) {
				return nil, nil, errors.New("dialed")
			}})
		if code != 0 {
			t.Fatalf("exit %d", code)
		}
		return strings.TrimSpace(out.String())
	}
	if got := versionOf(stamped("v0.0.2")); got != "grpcprocctl v0.0.2" {
		t.Fatal(got)
	}
	// Built from a checkout, or without build info: dev.
	if got := versionOf(stamped("(devel)")); got != "grpcprocctl dev" {
		t.Fatal(got)
	}
	if got := versionOf(func() (*debug.BuildInfo, bool) { return nil, false }); got != "grpcprocctl dev" {
		t.Fatal(got)
	}
	// -ldflags -X overrides the build info.
	cli.Version = "v1.2.3-custom"
	defer func() { cli.Version = "" }()
	if got := versionOf(stamped("v0.0.2")); got != "grpcprocctl v1.2.3-custom" {
		t.Fatal(got)
	}
	// This binary's own build info, read the default way.
	cli.Version = ""
	var out bytes.Buffer
	if code := cli.Main(t.Context(), []string{"--version"}, cli.Env{Stdout: &out}); code != 0 || !strings.HasPrefix(out.String(), "grpcprocctl ") {
		t.Fatalf("%d %q", code, out.String())
	}
}
