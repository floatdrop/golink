package cli_test

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"net"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/floatdrop/grpcproc/tools/cli"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
	"github.com/floatdrop/grpcproc/tools/web"
)

func TestWebCommand(t *testing.T) {
	f := testcluster.Start(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var stdout, stderr bytes.Buffer
	var asked string
	done := make(chan int, 1)
	go func() {
		done <- cli.Main(ctx, []string{"--addr", "a:9000", "web", "--listen", "localhost:0", "--allow-writes"}, cli.Env{
			Stdout: &stdout, Stderr: &stderr,
			Dial: func(context.Context, cli.Conn) (grpc.ClientConnInterface, func() error, error) {
				return f.C.Conn("a"), func() error { return nil }, nil
			},
			Listen: func(_ context.Context, addr string) (net.Listener, error) {
				asked = addr
				return ln, nil
			},
			Getenv: func(string) string { return "" },
		})
	}()
	// The UI answers, says what it reads and that it may change things,
	// and serves localhost only, as it listens there.
	var info web.Info
	deadline := time.Now().Add(5 * time.Second)
	for {
		res, err := http.Get("http://" + ln.Addr().String() + "/api/info")
		if err == nil {
			err = json.UnmarshalRead(res.Body, &info)
			_ = res.Body.Close()
			if err != nil || !info.AllowWrites || info.Target != "a:9000" {
				t.Fatalf("%+v %v", info, err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+ln.Addr().String()+"/api/info", nil)
	req.Host = "evil.test"
	if res, err := http.DefaultClient.Do(req); err != nil || res.StatusCode != http.StatusMisdirectedRequest {
		t.Fatalf("Host evil.test: %v %v", res, err)
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 || asked != "localhost:0" {
			t.Fatalf("exit %d, listened on %q: %s", code, asked, stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("web did not stop")
	}
	has(t, stderr.String(), "grpcprocctl web: a:9000 on http://"+ln.Addr().String())
}

func TestWebFailures(t *testing.T) {
	f := testcluster.Start(t)
	// An address that cannot be listened on.
	if r := run(t, f, "web", "--listen", "256.0.0.1:1"); r.code != 1 || r.stderr == "" {
		t.Fatalf("%+v", r)
	}
	if r := run(t, f, "web", "--port", "1"); r.code != 2 {
		t.Fatalf("%+v", r)
	}
	// A listener that fails to accept ends the command with its error.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = ln.Close()
	var stderr bytes.Buffer
	code := cli.Main(t.Context(), []string{"web"}, cli.Env{
		Stdout: &bytes.Buffer{}, Stderr: &stderr,
		Dial: func(context.Context, cli.Conn) (grpc.ClientConnInterface, func() error, error) {
			return f.C.Conn("a"), func() error { return nil }, nil
		},
		Listen: func(context.Context, string) (net.Listener, error) { return ln, nil },
		Getenv: func(string) string { return "" },
	})
	if code != 1 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
}
