// Package cli is golinkctl: inspect and operate golink nodes through their
// Inspector, from a terminal or, with `golinkctl mcp`, from an AI agent.
package cli

import (
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/floatdrop/golink/tools/client"
	"github.com/floatdrop/golink/tools/dot"
	"github.com/floatdrop/golink/tools/mcpserver"
)

// Conn says how to reach an Inspector.
type Conn struct {
	Addr       string
	Plaintext  bool
	CACert     string
	Cert, Key  string
	ServerName string
}

// Env is what golinkctl talks to. Main fills what is left zero with the
// process's own: stdout, stderr, a gRPC dial, MCP over stdio.
type Env struct {
	Stdout, Stderr io.Writer
	Dial           func(ctx context.Context, c Conn) (grpc.ClientConnInterface, func() error, error)
	MCPTransport   mcp.Transport
	Getenv         func(string) string
}

// Version is reported by golinkctl mcp.
var Version = "dev"

const usage = `golinkctl inspects and operates golink nodes through their Inspector.

Usage: golinkctl [flags] <command> [command flags] [args]

Commands:
  node [name]                 a node: counters and links
  nodes                       every node reachable from this one
  ps                          processes (--node, --name, --label, --state, --min-mailbox, --sort, --limit)
  inspect <pid|name>          one process, and what it says about itself (--node, --wait)
  watch                       stream events (--node, --kind, --count)
  exit <pid|name> [reason]    ask a process to exit (--node)
  loglevel <pid|name> <level> set a process's log level (--node)
  dot                         Graphviz of processes and who started whom (--node, --cluster)
  mcp                         serve these as MCP tools over stdio (--allow-writes)

A pid is written as golink prints it, <node.incarnation.id>; a name is
looked up on --node, by default the node serving the Inspector.

Flags:
`

type app struct {
	env     Env
	conn    Conn
	timeout time.Duration
	json    bool
	client  *client.Client
}

// Main runs golinkctl with args (without the program name) and returns the
// exit code: 0, 1 for a failed command, 2 for bad usage.
func Main(ctx context.Context, args []string, env Env) int {
	env.Stdout = cmp.Or[io.Writer](env.Stdout, os.Stdout)
	env.Stderr = cmp.Or[io.Writer](env.Stderr, os.Stderr)
	if env.Dial == nil {
		env.Dial = dial
	}
	if env.MCPTransport == nil {
		env.MCPTransport = &mcp.StdioTransport{}
	}
	if env.Getenv == nil {
		env.Getenv = os.Getenv
	}
	a := &app{env: env}
	fs := flag.NewFlagSet("golinkctl", flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	fs.Usage = func() {
		fmt.Fprint(env.Stderr, usage)
		fs.PrintDefaults()
	}
	fs.StringVar(&a.conn.Addr, "addr", cmp.Or(env.Getenv("GOLINK_ADDR"), "localhost:9000"), "Inspector address (default $GOLINK_ADDR)")
	fs.BoolVar(&a.conn.Plaintext, "plaintext", false, "connect without TLS")
	fs.StringVar(&a.conn.CACert, "cacert", "", "CA certificate file to verify the server")
	fs.StringVar(&a.conn.Cert, "cert", "", "client certificate file, for mutual TLS")
	fs.StringVar(&a.conn.Key, "key", "", "client key file, for mutual TLS")
	fs.StringVar(&a.conn.ServerName, "servername", "", "server name to verify, when it differs from the address")
	fs.DurationVar(&a.timeout, "timeout", 5*time.Second, "time limit for each request")
	fs.BoolVar(&a.json, "json", false, "print JSON instead of tables")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return 2
	}
	name, rest := fs.Arg(0), fs.Args()[1:]
	cmd, ok := commands[name]
	if !ok {
		fmt.Fprintf(env.Stderr, "golinkctl: unknown command %q\n\n", name)
		fs.Usage()
		return 2
	}
	cc, closeConn, err := env.Dial(ctx, a.conn)
	if err != nil {
		fmt.Fprintf(env.Stderr, "golinkctl: %v\n", err)
		return 1
	}
	defer func() { _ = closeConn() }()
	a.client = client.New(cc)
	if err := cmd(ctx, a, rest); err != nil {
		if _, ok := errors.AsType[flagError](err); ok {
			return 2 // the flag package has said what was wrong
		}
		if _, ok := errors.AsType[usageError](err); ok {
			fmt.Fprintf(env.Stderr, "golinkctl %s: %v\n", name, err)
			return 2
		}
		fmt.Fprintf(env.Stderr, "golinkctl %s: %s\n", name, status.Convert(err).Message())
		return 1
	}
	return 0
}

type usageError struct{ msg string }

func (u usageError) Error() string { return u.msg }

type flagError struct{ error }

func dial(_ context.Context, c Conn) (grpc.ClientConnInterface, func() error, error) {
	creds, err := c.credentials()
	if err != nil {
		return nil, nil, err
	}
	cc, err := grpc.NewClient(c.Addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, err
	}
	return cc, cc.Close, nil
}

func (c Conn) credentials() (credentials.TransportCredentials, error) {
	if c.Plaintext {
		return insecure.NewCredentials(), nil
	}
	cfg := &tls.Config{ServerName: c.ServerName, MinVersion: tls.VersionTLS12}
	if c.CACert != "" {
		pem, err := os.ReadFile(c.CACert)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates in %s", c.CACert)
		}
	}
	if c.Cert != "" || c.Key != "" {
		if c.Cert == "" || c.Key == "" {
			return nil, errors.New("--cert and --key go together")
		}
		pair, err := tls.LoadX509KeyPair(c.Cert, c.Key)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return credentials.NewTLS(cfg), nil
}

type command func(ctx context.Context, a *app, args []string) error

var commands map[string]command

func init() {
	commands = map[string]command{
		"node": cmdNode, "nodes": cmdNodes, "ps": cmdPS, "inspect": cmdInspect, "watch": cmdWatch,
		"exit": cmdExit, "loglevel": cmdLogLevel, "dot": cmdDot, "mcp": cmdMCP,
	}
}

// flags parses a command's flags; positional arguments follow them.
func (a *app) flags(name string, args []string, define func(*flag.FlagSet)) (*flag.FlagSet, error) {
	fs := flag.NewFlagSet("golinkctl "+name, flag.ContinueOnError)
	fs.SetOutput(a.env.Stderr)
	define(fs)
	if err := fs.Parse(args); err != nil {
		return nil, flagError{err}
	}
	return fs, nil
}

func (a *app) request(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, a.timeout)
}

func (a *app) printJSON(v any) error {
	return json.MarshalWrite(a.env.Stdout, v, jsontext.WithIndentPrefix(""), jsontext.WithIndent("  "))
}

func (a *app) table(header string, rows [][]string) error {
	w := tabwriter.NewWriter(a.env.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, header)
	for _, r := range rows {
		fmt.Fprintln(w, strings.Join(r, "\t"))
	}
	return w.Flush()
}

func u(v uint64) string { return strconv.FormatUint(v, 10) }

func cmdNode(ctx context.Context, a *app, args []string) error {
	fs, err := a.flags("node", args, func(*flag.FlagSet) {})
	if err != nil {
		return err
	}
	ctx, cancel := a.request(ctx)
	defer cancel()
	n, err := a.client.Node(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	if a.json {
		return a.printJSON(n)
	}
	fmt.Fprintf(a.env.Stdout, "node:          %s#%d\nadvertise:     %s\nuptime:        %s\nprocesses:     %d (spawned %d, exited %d)\ndead letters:  %d\n\n",
		n.Name, n.Incarnation, n.Advertise, n.Uptime, n.Processes, n.Spawned, n.Exited, n.DeadLetters)
	rows := make([][]string, 0, len(n.Links))
	for _, l := range n.Links {
		rows = append(rows, []string{l.Peer + "#" + u(l.Incarnation), l.Direction, l.State, l.Age, u(l.Messages), u(l.Bytes), u(l.Reconnects), l.LastError})
	}
	return a.table("PEER\tDIR\tSTATE\tAGE\tMESSAGES\tBYTES\tRECONNECTS\tLAST ERROR", rows)
}

func cmdNodes(ctx context.Context, a *app, args []string) error {
	if _, err := a.flags("nodes", args, func(*flag.FlagSet) {}); err != nil {
		return err
	}
	ctx, cancel := a.request(ctx)
	defer cancel()
	nodes, err := a.client.Cluster(ctx)
	if err != nil {
		return err
	}
	if a.json {
		return a.printJSON(nodes)
	}
	rows := make([][]string, 0, len(nodes))
	for _, n := range nodes {
		peers := make([]string, 0, len(n.Links))
		for _, l := range n.Links {
			if !slices.Contains(peers, l.Peer) {
				peers = append(peers, l.Peer)
			}
		}
		rows = append(rows, []string{n.Name, n.Advertise, n.Uptime, strconv.Itoa(n.Processes), u(n.DeadLetters), strings.Join(peers, ","), n.Error})
	}
	return a.table("NODE\tADVERTISE\tUPTIME\tPROCESSES\tDEAD LETTERS\tPEERS\tERROR", rows)
}

func cmdPS(ctx context.Context, a *app, args []string) error {
	var node, sortBy string
	var limit int
	var f client.Filter
	if _, err := a.flags("ps", args, func(fs *flag.FlagSet) {
		fs.StringVar(&node, "node", "", "node to list, by default the one serving the Inspector")
		fs.StringVar(&f.Name, "name", "", "only processes with a registered name containing this")
		fs.StringVar(&f.Label, "label", "", "only processes with this label")
		fs.StringVar(&f.State, "state", "", "only processes in this state: idle, running, waiting-reply, exiting")
		fs.IntVar(&f.MinMailbox, "min-mailbox", 0, "only processes with at least this many waiting messages")
		fs.StringVar(&sortBy, "sort", "pid", "order: pid, mailbox, received or sent")
		fs.IntVar(&limit, "limit", 0, "show at most this many (0: all)")
	}); err != nil {
		return err
	}
	ctx, cancel := a.request(ctx)
	defer cancel()
	ps, err := a.client.Processes(ctx, node, f)
	if err != nil {
		return err
	}
	if err := client.SortProcesses(ps, sortBy); err != nil {
		return usageError{err.Error()}
	}
	if limit > 0 && len(ps) > limit {
		ps = ps[:limit]
	}
	if a.json {
		return a.printJSON(ps)
	}
	rows := make([][]string, 0, len(ps))
	for _, p := range ps {
		rows = append(rows, []string{p.PID, strings.Join(p.Names, ","), p.Label, p.State, strconv.Itoa(p.Mailbox), p.OldestWait, u(p.Received), u(p.Sent), p.LastMessage, p.Uptime})
	}
	return a.table("PID\tNAME\tLABEL\tSTATE\tMAILBOX\tOLDEST\tRECEIVED\tSENT\tLAST MESSAGE\tUPTIME", rows)
}

func cmdInspect(ctx context.Context, a *app, args []string) error {
	var node string
	var wait time.Duration
	fs, err := a.flags("inspect", args, func(fs *flag.FlagSet) {
		fs.StringVar(&node, "node", "", "node the name is registered on")
		fs.DurationVar(&wait, "wait", time.Second, "how long to wait for a busy process to answer")
	})
	if err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return usageError{"want one process: <node.incarnation.id> or a name"}
	}
	ctx, cancel := a.request(ctx)
	defer cancel()
	p, err := a.client.Process(ctx, node, fs.Arg(0), true, wait)
	if err != nil {
		return err
	}
	if a.json {
		return a.printJSON(p)
	}
	w := tabwriter.NewWriter(a.env.Stdout, 0, 0, 2, ' ', 0)
	for _, kv := range [][2]string{
		{"pid", p.PID}, {"names", strings.Join(p.Names, ", ")}, {"label", p.Label}, {"type", p.Type},
		{"parent", p.Parent}, {"state", p.State}, {"uptime", p.Uptime},
		{"mailbox", fmt.Sprintf("%d (peak %d, oldest %s)", p.Mailbox, p.MailboxPeak, cmp.Or(p.OldestWait, "-"))},
		{"received", u(p.Received)}, {"sent", u(p.Sent)}, {"calls in flight", strconv.Itoa(int(p.CallsInFlight))},
		{"last message", p.LastMessage}, {"monitors", strconv.Itoa(p.Monitors)}, {"watchers", strconv.Itoa(p.Watchers)},
		{"log level", p.LogLevel},
	} {
		fmt.Fprintf(w, "%s:\t%s\n", kv[0], kv[1])
	}
	if p.InspectError != "" {
		fmt.Fprintf(w, "inspect:\t%s\n", p.InspectError)
	}
	for _, k := range slices.Sorted(maps.Keys(p.Inspect)) {
		fmt.Fprintf(w, "  %s:\t%s\n", k, p.Inspect[k])
	}
	return w.Flush()
}

func cmdWatch(ctx context.Context, a *app, args []string) error {
	var node, kinds string
	var count int
	if _, err := a.flags("watch", args, func(fs *flag.FlagSet) {
		fs.StringVar(&node, "node", "", "node to watch")
		fs.StringVar(&kinds, "kind", "", "only these kinds, comma separated: spawn, exit, link-up, link-down, dead-letter")
		fs.IntVar(&count, "count", 0, "stop after this many events (0: until interrupted)")
	}); err != nil {
		return err
	}
	var only []string
	if kinds != "" {
		only = strings.Split(kinds, ",")
	}
	seen := 0
	var werr error
	err := a.client.Watch(ctx, node, func(e client.EventView) bool {
		if only != nil && !slices.Contains(only, e.Kind) {
			return true
		}
		if a.json {
			werr = json.MarshalWrite(a.env.Stdout, e)
			fmt.Fprintln(a.env.Stdout)
		} else {
			fmt.Fprintln(a.env.Stdout, eventLine(e))
		}
		seen++
		return werr == nil && (count == 0 || seen < count)
	})
	return cmp.Or(werr, err)
}

func eventLine(e client.EventView) string {
	t := e.Time
	if ts, err := time.Parse(time.RFC3339Nano, e.Time); err == nil {
		t = ts.Format("15:04:05.000")
	}
	parts := []string{t, e.Kind}
	if e.Process != nil {
		parts = append(parts, e.Process.PID, "label="+e.Process.Label)
		if len(e.Process.Names) > 0 {
			parts = append(parts, "name="+strings.Join(e.Process.Names, ","))
		}
	}
	for _, kv := range [][2]string{{"peer", e.Peer}, {"from", e.From}, {"to", e.To}, {"type", e.Type}, {"reason", e.Reason}, {"error", e.Error}} {
		if kv[1] != "" {
			parts = append(parts, kv[0]+"="+strconv.Quote(kv[1]))
		}
	}
	if e.Missed > 0 {
		parts = append(parts, "missed="+u(e.Missed))
	}
	return strings.Join(parts, " ")
}

func cmdExit(ctx context.Context, a *app, args []string) error {
	var node string
	fs, err := a.flags("exit", args, func(fs *flag.FlagSet) { fs.StringVar(&node, "node", "", "node the name is registered on") })
	if err != nil {
		return err
	}
	if fs.NArg() < 1 || fs.NArg() > 2 {
		return usageError{"want a process and an optional reason"}
	}
	ctx, cancel := a.request(ctx)
	defer cancel()
	return a.client.Exit(ctx, node, fs.Arg(0), fs.Arg(1))
}

func cmdLogLevel(ctx context.Context, a *app, args []string) error {
	var node string
	fs, err := a.flags("loglevel", args, func(fs *flag.FlagSet) { fs.StringVar(&node, "node", "", "node the name is registered on") })
	if err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return usageError{"want a process and a level"}
	}
	if _, err := client.ParseLevel(fs.Arg(1)); err != nil {
		return usageError{err.Error()}
	}
	ctx, cancel := a.request(ctx)
	defer cancel()
	return a.client.SetLogLevel(ctx, node, fs.Arg(0), fs.Arg(1))
}

func cmdDot(ctx context.Context, a *app, args []string) error {
	var node string
	var cluster bool
	if _, err := a.flags("dot", args, func(fs *flag.FlagSet) {
		fs.StringVar(&node, "node", "", "node to draw")
		fs.BoolVar(&cluster, "cluster", false, "draw every node reachable from this one")
	}); err != nil {
		return err
	}
	ctx, cancel := a.request(ctx)
	defer cancel()
	var names []string
	if cluster {
		nodes, err := a.client.Cluster(ctx)
		if err != nil {
			return err
		}
		for _, n := range nodes {
			if n.Error == "" {
				names = append(names, n.Name)
			}
		}
	} else {
		n, err := a.client.Node(ctx, node)
		if err != nil {
			return err
		}
		names = []string{n.Name}
	}
	graph := make([]dot.Node, 0, len(names))
	for _, name := range names {
		ps, err := a.client.Processes(ctx, name, client.Filter{})
		if err != nil {
			return err
		}
		graph = append(graph, dot.Node{Name: name, Processes: ps})
	}
	return dot.Render(a.env.Stdout, graph)
}

func cmdMCP(ctx context.Context, a *app, args []string) error {
	var writes bool
	if _, err := a.flags("mcp", args, func(fs *flag.FlagSet) {
		fs.BoolVar(&writes, "allow-writes", false, "also offer tools that change things: exit a process, set a log level")
	}); err != nil {
		return err
	}
	s := mcpserver.New(a.client, mcpserver.Options{AllowWrites: writes, Version: Version, Timeout: a.timeout})
	if err := s.Run(ctx, a.env.MCPTransport); err != nil && ctx.Err() == nil {
		return err
	}
	return nil // interrupted, or the client went away
}
