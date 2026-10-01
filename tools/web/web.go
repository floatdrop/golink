// Package web is grpcprocctl's web UI: a page in the browser that shows a
// grpcproc cluster through its Inspector, live, as Erlang's observer shows a
// BEAM node. It serves the page and the API behind it from one handler; the
// API returns the objects grpcprocctl --json prints and its MCP tools
// return, and streams a node's events as server-sent events.
//
//	grpcprocctl --plaintext web              # http://localhost:9911
//	grpcprocctl --plaintext web --allow-writes
package web

import (
	"cmp"
	"context"
	"embed"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/floatdrop/grpcproc/tools/client"
)

//go:embed ui
var files embed.FS

// Options configures New.
type Options struct {
	// AllowWrites also serves the requests that change things: exit a
	// process, set its log level, move or cordon a leader, change a cron
	// job. Without it they are refused, and the page does not offer them.
	AllowWrites bool
	// Version and Target (the Inspector's address) are shown in the page.
	Version, Target string
	// Timeout bounds each Inspector request. Default 5s.
	Timeout time.Duration
	// LoopbackOnly refuses a request whose Host is not localhost or a
	// loopback address. On a loopback listener it keeps a page on another
	// site, whose name that site has pointed at 127.0.0.1 (DNS rebinding),
	// from reading the cluster through the browser.
	LoopbackOnly bool
}

// maxWait bounds how long a request waits for a busy process to answer.
const maxWait = 10 * time.Second

type server struct {
	c *client.Client
	o Options
}

// New returns the page and its API over c.
func New(c *client.Client, o Options) http.Handler {
	o.Timeout = cmp.Or(max(o.Timeout, 0), 5*time.Second)
	s := &server{c: c, o: o}
	ui, _ := fs.Sub(files, "ui") // ui is a directory of files: Sub cannot fail
	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServerFS(ui))
	mux.HandleFunc("GET /api/info", s.read(s.info))
	mux.HandleFunc("GET /api/nodes", s.read(func(ctx context.Context, _ url.Values) (any, error) { return s.c.Cluster(ctx) }))
	mux.HandleFunc("GET /api/node", s.read(func(ctx context.Context, q url.Values) (any, error) { return s.c.Node(ctx, q.Get("node")) }))
	mux.HandleFunc("GET /api/processes", s.read(s.processes))
	mux.HandleFunc("GET /api/process", s.read(s.process))
	mux.HandleFunc("GET /api/names", s.read(func(ctx context.Context, q url.Values) (any, error) {
		limit, _ := strconv.Atoi(q.Get("limit")) // 0, the node's default, unless a number
		return s.c.Names(ctx, q.Get("node"), q.Get("prefix"), limit)
	}))
	mux.HandleFunc("GET /api/crons", s.read(func(ctx context.Context, q url.Values) (any, error) { return s.c.Crons(ctx, q.Get("node")) }))
	mux.HandleFunc("GET /api/elections", s.read(func(ctx context.Context, _ url.Values) (any, error) { return s.c.Elections(ctx) }))
	mux.HandleFunc("GET /api/events", s.events)
	mux.HandleFunc("POST /api/exit", write(s, s.exit))
	mux.HandleFunc("POST /api/loglevel", write(s, s.logLevel))
	mux.HandleFunc("POST /api/cron", write(s, s.cron))
	mux.HandleFunc("POST /api/leader", write(s, s.leader))
	// A POST from a page on another origin is refused before it reaches a
	// handler: the browser says where it comes from (Sec-Fetch-Site, Origin).
	h := http.NewCrossOriginProtection().Handler(mux)
	if o.LoopbackOnly {
		h = loopback(h)
	}
	return headers(h)
}

// headers keeps the page to its own scripts and styles, and out of frames.
func headers(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func loopback(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]") // [::1] on port 80
		if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			http.Error(w, fmt.Sprintf("grpcprocctl web serves localhost only, not %q", r.Host), http.StatusMisdirectedRequest)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// read serves a GET: fn's answer as JSON, within the time limit, plus the
// wait for a busy process when the request asks to wait.
func (s *server) read(fn func(context.Context, url.Values) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		wait, err := waitOf(q)
		if err != nil {
			reply(w, nil, err)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), s.o.Timeout+wait)
		defer cancel()
		v, err := fn(ctx, q)
		reply(w, v, err)
	}
}

// write serves a POST whose JSON body is a T, if writes are allowed.
func write[T any](s *server, fn func(context.Context, T) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.o.AllowWrites {
			reply(w, nil, status.Error(codes.PermissionDenied, "read-only: start grpcprocctl web with --allow-writes to change things"))
			return
		}
		var req T
		if err := json.UnmarshalRead(http.MaxBytesReader(w, r.Body, 1<<16), &req); err != nil {
			reply(w, nil, badRequest{"bad request body: " + err.Error()})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), s.o.Timeout)
		defer cancel()
		v, err := fn(ctx, req)
		reply(w, v, err)
	}
}

// badRequest is a request that is wrong in itself, before any node sees it.
type badRequest struct{ msg string }

func (b badRequest) Error() string { return b.msg }

// reply writes v as JSON, or err as {"error": ...} with a status that says
// whose fault it is.
func reply(w http.ResponseWriter, v any, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		w.WriteHeader(httpStatus(err))
		v = map[string]string{"error": status.Convert(err).Message()}
	}
	_ = json.MarshalWrite(w, v, json.Deterministic(true))
}

func httpStatus(err error) int {
	if _, ok := errors.AsType[badRequest](err); ok {
		return http.StatusBadRequest
	}
	st, ok := status.FromError(err)
	if !ok {
		// The client checks what it is asked before sending it: a pid, a
		// state, a level. What it refuses is the request's fault.
		return http.StatusBadRequest
	}
	switch st.Code() {
	case codes.NotFound:
		return http.StatusNotFound
	case codes.InvalidArgument, codes.FailedPrecondition:
		return http.StatusBadRequest
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.Unavailable:
		return http.StatusBadGateway
	case codes.Unknown:
		// What a process answered to a Call with an error: the request
		// reached it, and it said no.
		return http.StatusConflict
	}
	return http.StatusInternalServerError
}

func waitOf(q url.Values) (time.Duration, error) {
	if q.Get("wait") == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(q.Get("wait"))
	if err != nil || d < 0 || d > maxWait {
		return 0, badRequest{fmt.Sprintf("bad wait %q: want a duration up to %v", q.Get("wait"), maxWait)}
	}
	return d, nil
}

// Info is what the page shows about the tool itself.
type Info struct {
	Version     string `json:"version"`
	Target      string `json:"target"`
	AllowWrites bool   `json:"allow_writes"`
}

func (s *server) info(context.Context, url.Values) (any, error) {
	return Info{Version: cmp.Or(s.o.Version, "dev"), Target: s.o.Target, AllowWrites: s.o.AllowWrites}, nil
}

func (s *server) processes(ctx context.Context, q url.Values) (any, error) {
	f := client.Filter{Name: q.Get("name"), Label: q.Get("label"), State: q.Get("state")}
	if m := q.Get("min_mailbox"); m != "" {
		var err error
		if f.MinMailbox, err = strconv.Atoi(m); err != nil {
			return nil, badRequest{fmt.Sprintf("bad min_mailbox %q: want a number", m)}
		}
	}
	return s.c.Processes(ctx, q.Get("node"), f)
}

// process is one process; with inspect=1, also what it says about itself,
// waiting up to wait (1s by default) while it is busy.
func (s *server) process(ctx context.Context, q url.Values) (any, error) {
	wait, _ := waitOf(q) // read has checked it
	return s.c.Process(ctx, q.Get("node"), q.Get("target"), q.Get("inspect") == "1", wait)
}

// events streams node's events, one JSON object per event. If the node
// cannot be reached, or the Inspector ends the stream, a "failure" event says
// why and the browser's EventSource connects again.
func (s *server) events(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, err := fmt.Fprint(w, "retry: 3000\n\n")
	if err == nil {
		err = rc.Flush()
	}
	if err != nil {
		return
	}
	// A node that cannot be reached would hold the stream open with nothing
	// on it, as if all were quiet: it is asked first, within the time limit.
	node := r.URL.Query().Get("node")
	ctx, cancel := context.WithTimeout(r.Context(), s.o.Timeout)
	_, err = s.c.Node(ctx, node)
	cancel()
	if err == nil {
		err = s.c.Watch(r.Context(), node, func(e client.EventView) bool {
			b, _ := json.Marshal(e) // an EventView always marshals
			_, err := fmt.Fprintf(w, "data: %s\n\n", b)
			return err == nil && rc.Flush() == nil
		})
	}
	if err != nil {
		b, _ := json.Marshal(status.Convert(err).Message())
		_, _ = fmt.Fprintf(w, "event: failure\ndata: %s\n\n", b)
	}
}

// ExitRequest asks a process to exit.
type ExitRequest struct {
	Node   string `json:"node"`
	Target string `json:"target"`
	Reason string `json:"reason"`
}

func (s *server) exit(ctx context.Context, req ExitRequest) (any, error) {
	return struct{}{}, s.c.Exit(ctx, req.Node, req.Target, req.Reason)
}

// LogLevelRequest sets a process's log level.
type LogLevelRequest struct {
	Node   string `json:"node"`
	Target string `json:"target"`
	Level  string `json:"level"`
}

func (s *server) logLevel(ctx context.Context, req LogLevelRequest) (any, error) {
	level, err := client.ParseLevel(req.Level)
	if err != nil {
		return nil, err
	}
	return struct{}{}, s.c.SetLogLevel(ctx, req.Node, req.Target, level)
}

// CronRequest changes a job of a cron process: Op is enable, disable or
// remove.
type CronRequest struct {
	Node   string `json:"node"`
	Target string `json:"target"`
	Op     string `json:"op"`
	Job    string `json:"job"`
}

func (s *server) cron(ctx context.Context, req CronRequest) (any, error) {
	return struct{}{}, s.c.CronJob(ctx, req.Node, req.Target, req.Op, req.Job)
}

// LeaderRequest changes an election through its leader: Op move hands
// leadership To a node (by default the follower with the latest state);
// cordon and uncordon keep Node from leading, or let it again.
type LeaderRequest struct {
	Cluster string `json:"cluster"`
	Op      string `json:"op"`
	Node    string `json:"node"`
	To      string `json:"to"`
}

// LeaderResponse names the node that led when the change was made.
type LeaderResponse struct {
	Led string `json:"led"`
}

func (s *server) leader(ctx context.Context, req LeaderRequest) (any, error) {
	var led string
	var err error
	switch req.Op {
	case "move":
		led, err = s.c.MoveLeader(ctx, req.Cluster, req.To)
	case "cordon", "uncordon":
		led, err = s.c.Cordon(ctx, req.Cluster, req.Node, req.Op == "uncordon")
	default:
		return nil, badRequest{fmt.Sprintf("bad op %q: want move, cordon or uncordon", req.Op)}
	}
	if err != nil {
		return nil, err
	}
	return LeaderResponse{Led: led}, nil
}
