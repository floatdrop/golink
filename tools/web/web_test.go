package web_test

import (
	"bufio"
	"context"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
	"github.com/floatdrop/grpcproc/tools/web"
)

// serve runs the UI over the fixture's node a.
func serve(t *testing.T, f *testcluster.Fixture, o web.Options) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(web.New(client.New(f.C.Conn("a")), o))
	t.Cleanup(s.Close)
	return s
}

// get fetches path and decodes its JSON into out, returning the status.
func get(t *testing.T, s *httptest.Server, path string, out any) int {
	t.Helper()
	res, err := http.Get(s.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if out != nil {
		if err := json.UnmarshalRead(res.Body, out); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	return res.StatusCode
}

// post sends body as JSON to path and returns the status and the error it
// says, if any.
func post(t *testing.T, s *httptest.Server, path, body string, header ...string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, s.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	var e struct {
		Error string `json:"error"`
	}
	_ = json.UnmarshalRead(res.Body, &e) // a refusal from the cross-origin check is text
	return res.StatusCode, e.Error
}

func TestPage(t *testing.T) {
	f := testcluster.Start(t)
	s := serve(t, f, web.Options{})
	for path, want := range map[string]string{
		"/": "<title>grpcprocctl web</title>", "/app.js": "function pidParts", "/theme.js": "g-root_theme_",
		"/gravity.css": "--g-color-base-background", "/style.css": "var(--g-color-line-generic)", "/favicon.svg": "<svg",
	} {
		res, err := http.Get(s.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		if res.StatusCode != http.StatusOK || !strings.Contains(string(body), want) {
			t.Errorf("%s: %d, no %q", path, res.StatusCode, want)
		}
		if csp := res.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'self'") {
			t.Errorf("%s: CSP %q", path, csp)
		}
	}
}

func TestRead(t *testing.T) {
	f := testcluster.Start(t, "c")
	f.Cron(t, "b", "billing")
	f.Elect(t, "sched", "a", "b", "c")
	s := serve(t, f, web.Options{Version: "v1.2.3", Target: "a:9000"})

	var info web.Info
	if get(t, s, "/api/info", &info) != http.StatusOK || info != (web.Info{Version: "v1.2.3", Target: "a:9000"}) {
		t.Errorf("%+v", info)
	}
	var nodes []client.NodeView
	if get(t, s, "/api/nodes", &nodes); len(nodes) != 3 || nodes[0].Name != "a" {
		t.Errorf("%+v", nodes)
	}
	var node client.NodeView
	if get(t, s, "/api/node?node=b", &node); node.Name != "b" {
		t.Errorf("%+v", node)
	}
	var ps []client.ProcessView
	if get(t, s, "/api/processes?node=a&name=stu&min_mailbox=1", &ps); len(ps) != 1 || ps[0].Name != "stuck" {
		t.Errorf("%+v", ps)
	}
	var p client.ProcessView
	if get(t, s, "/api/process?target=talker&inspect=1&wait=50ms", &p); p.Inspect["state"] != "ready" {
		t.Errorf("%+v", p)
	}
	if get(t, s, "/api/process?target=stuck&inspect=1&wait=10ms", &p); !strings.Contains(p.InspectError, "busy") {
		t.Errorf("stuck: %+v", p)
	}
	var crons []client.CronView
	if get(t, s, "/api/crons", &crons); len(crons) != 1 || crons[0].Process != "billing" || len(crons[0].Jobs) != 3 {
		t.Errorf("%+v", crons)
	}
	var elections []client.ElectionView
	if get(t, s, "/api/elections", &elections); len(elections) != 1 || elections[0].Cluster != "sched" || len(elections[0].Electors) != 3 {
		t.Errorf("%+v", elections)
	}

	// What is wrong with a request says so, with a status that says whose
	// fault it is.
	for path, want := range map[string]int{
		"/api/processes?min_mailbox=many":         http.StatusBadRequest,
		"/api/processes?state=asleep":             http.StatusBadRequest,
		"/api/process?target=talker&wait=forever": http.StatusBadRequest,
		"/api/process?target=talker&wait=1h":      http.StatusBadRequest,
		"/api/process?target=nobody":              http.StatusNotFound,
		"/api/node?node=nowhere":                  http.StatusBadGateway,
	} {
		var e struct {
			Error string `json:"error"`
		}
		if got := get(t, s, path, &e); got != want || e.Error == "" {
			t.Errorf("%s: %d %q, want %d", path, got, e.Error, want)
		}
	}
}

func TestWrite(t *testing.T) {
	f := testcluster.Start(t, "c")
	pid := f.Cron(t, "b", "billing")
	f.Elect(t, "sched", "a", "b", "c")

	// Read-only unless asked.
	ro := serve(t, f, web.Options{})
	if code, msg := post(t, ro, "/api/exit", `{"target":"talker"}`); code != http.StatusForbidden || !strings.Contains(msg, "--allow-writes") {
		t.Errorf("read-only exit: %d %q", code, msg)
	}

	s := serve(t, f, web.Options{AllowWrites: true})
	for _, tc := range []struct {
		path, body string
		want       int
	}{
		{"/api/loglevel", `{"target":"talker","level":"debug"}`, http.StatusOK},
		{"/api/loglevel", `{"target":"talker","level":"loud"}`, http.StatusBadRequest},
		{"/api/cron", `{"node":"b","target":"billing","op":"disable","job":"yearly"}`, http.StatusOK},
		{"/api/cron", `{"target":"` + pid.String() + `","op":"remove","job":"nothing"}`, http.StatusConflict},
		{"/api/leader", `{"cluster":"sched","op":"cordon","node":"b"}`, http.StatusOK},
		{"/api/leader", `{"cluster":"sched","op":"uncordon","node":"b"}`, http.StatusOK},
		{"/api/leader", `{"cluster":"sched","op":"move"}`, http.StatusOK},
		{"/api/leader", `{"cluster":"sched","op":"abdicate"}`, http.StatusBadRequest},
		{"/api/leader", `{"cluster":"nothing","op":"move"}`, http.StatusBadRequest},
		{"/api/exit", `{"target":"talker","reason":"bye"}`, http.StatusOK},
		{"/api/exit", `{"target":`, http.StatusBadRequest},
	} {
		if code, msg := post(t, s, tc.path, tc.body); code != tc.want {
			t.Errorf("%s %s: %d %q, want %d", tc.path, tc.body, code, msg, tc.want)
		}
	}
	var crons []client.CronView
	if get(t, s, "/api/crons?node=b", &crons); len(crons) != 1 || !strings.Contains(jobs(crons[0]), "yearly disabled") {
		t.Errorf("%+v", crons)
	}

	// A page on another site cannot make the browser change things.
	if code, _ := post(t, s, "/api/exit", `{"target":"stuck"}`, "Sec-Fetch-Site", "cross-site"); code != http.StatusForbidden {
		t.Errorf("cross-site exit: %d", code)
	}
	var p client.ProcessView
	if get(t, s, "/api/process?target=stuck", &p) != http.StatusOK {
		t.Error("stuck was made to exit from another site")
	}
}

func jobs(c client.CronView) string {
	var out []string
	for _, j := range c.Jobs {
		state := "enabled"
		if j.Disabled {
			state = "disabled"
		}
		out = append(out, j.Name+" "+state)
	}
	return strings.Join(out, ", ")
}

func TestLoopbackOnly(t *testing.T) {
	f := testcluster.Start(t)
	h := web.New(client.New(f.C.Conn("a")), web.Options{LoopbackOnly: true})
	for host, want := range map[string]int{
		"localhost:9911":  http.StatusOK,
		"127.0.0.1:9911":  http.StatusOK,
		"[::1]:9911":      http.StatusOK,
		"[::1]":           http.StatusOK,
		"localhost":       http.StatusOK,
		"evil.test:9911":  http.StatusMisdirectedRequest,
		"10.0.0.5:9911":   http.StatusMisdirectedRequest,
		"localhost.evil.": http.StatusMisdirectedRequest,
	} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/info", nil)
		req.Host = host
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != want {
			t.Errorf("Host %s: %d, want %d", host, w.Code, want)
		}
	}
}

func TestEvents(t *testing.T) {
	f := testcluster.Start(t)
	s := serve(t, f, web.Options{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.URL+"/api/events?node=a", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if ct := res.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type %q", ct)
	}
	lines := bufio.NewScanner(res.Body)
	if !lines.Scan() || lines.Text() != "retry: 3000" {
		t.Fatalf("first line %q", lines.Text())
	}
	// The watch opens after the first line is sent: short-lived processes
	// start until one is seen to start and end.
	go func() {
		for ctx.Err() == nil {
			_, _ = f.C.Node("a").Spawn(func(*grpcproc.Process[proto.Message]) error { return nil }, grpcproc.WithLabel("brief"))
			time.Sleep(10 * time.Millisecond)
		}
	}()
	var kinds []string
	var first string
	for len(kinds) < 2 && lines.Scan() {
		data, ok := strings.CutPrefix(lines.Text(), "data: ")
		if !ok {
			continue
		}
		var e client.EventView
		if err := json.Unmarshal([]byte(data), &e); err != nil {
			t.Fatal(err)
		}
		if e.Process == nil || e.Process.Label != "brief" || first != "" && e.Process.PID != first {
			continue
		}
		if first == "" && e.Kind != "spawn" {
			continue // its spawn went before the watch
		}
		first = e.Process.PID
		kinds = append(kinds, e.Kind)
	}
	if strings.Join(kinds, ",") != "spawn,exit" {
		t.Errorf("events of brief: %v", kinds)
	}

	// A node that cannot be watched: the stream says why and ends.
	res, err = http.Get(s.URL + "/api/events?node=nowhere")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if !strings.Contains(string(body), "event: failure\ndata: \"") {
		t.Errorf("%q", body)
	}

	// A writer that cannot flush cannot stream: nothing more is sent.
	w := httptest.NewRecorder()
	web.New(client.New(f.C.Conn("a")), web.Options{}).ServeHTTP(struct{ http.ResponseWriter }{w}, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/events", nil))
	if w.Body.String() != "retry: 3000\n\n" {
		t.Errorf("%q", w.Body.String())
	}
}
