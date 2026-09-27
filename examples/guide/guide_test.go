package guide

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"text/tabwriter"
	"time"

	"golang.yandex/di"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/guide/internal/inventory"
	"github.com/floatdrop/grpcproc/examples/guide/internal/orders"
	"github.com/floatdrop/grpcproc/examples/guide/internal/payments"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	"github.com/floatdrop/grpcproc/examples/guide/internal/web"
)

var update = flag.Bool("update", false, "rewrite testdata/ from what the programs do")

// The compositions of the four entry points. A test cannot import package
// main, so **these are copies of cmd/*/main.go**: change a main and change
// it here, or the tests, and the output the guide shows, describe programs
// that are not the ones that run.
var (
	local     = []di.Module{inventory.Module, payments.Module, orders.Module, web.Module}
	front     = []di.Module{orders.Module, web.Module}
	warehouse = []di.Module{inventory.Module}
	billing   = []di.Module{payments.Module}
)

// compose is platform.Run without the program: the same composition, a
// silent logger, and an HTTP port the system picks.
func compose(cfg platform.Config, services []di.Module) *di.Scope {
	cfg.Listen = cmp.Or(cfg.Listen, "127.0.0.1:0")
	cfg.HTTP = "127.0.0.1:0"
	app := di.New()
	platform.Compose(app, cfg, slog.New(slog.DiscardHandler), services...)
	return app
}

func start(t *testing.T, app *di.Scope) {
	t.Helper()
	if err := app.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Not t.Context(): it is cancelled before cleanups run.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := app.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
}

var mains = map[string][]di.Module{"local": local, "front": front, "warehouse": warehouse, "billing": billing}

func TestWiringValidates(t *testing.T) {
	for name, services := range mains {
		if err := compose(platform.Config{Node: name}, services).Validate().Err(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// The copies above are checked against the mains: each passes platform.Run
// the same modules, in the same order.
func TestCopiesMatchTheMains(t *testing.T) {
	for name, services := range mains {
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("cmd", name, "main.go"), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		var passed []string
		ast.Inspect(file, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok && types.ExprString(call.Fun) == "platform.Run" {
				for _, arg := range call.Args {
					passed = append(passed, types.ExprString(arg))
				}
			}
			return true
		})
		var copied []string
		for _, m := range services {
			full := runtime.FuncForPC(reflect.ValueOf(m).Pointer()).Name() // .../internal/web.Module
			copied = append(copied, full[strings.LastIndex(full, "/")+1:])
		}
		if !slices.Equal(passed, copied) {
			t.Errorf("cmd/%s passes %v, the test composes %v", name, passed, copied)
		}
	}
}

// The module report the guide shows is this test's output.
func TestModulesMatchTheGuide(t *testing.T) {
	golden(t, "modules.txt", compose(platform.Config{Node: "shop"}, local).Modules())
}

// order posts to the web front's handler, and returns the status and body.
func order(t *testing.T, app *di.Scope, body string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/orders", strings.NewReader(body))
	app.Get[*http.Server]().Handler.ServeHTTP(rec, req)
	return rec.Code, strings.TrimSpace(rec.Body.String())
}

// tree draws what a node runs, from its processes' parents.
func tree(n *grpcproc.Node) string {
	children := map[grpcproc.PID][]grpcproc.ProcessInfo{}
	for _, p := range n.Processes() {
		children[p.Parent] = append(children[p.Parent], p)
	}
	var b strings.Builder
	b.WriteString(n.Name() + "\n")
	w := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	var walk func(parent grpcproc.PID, indent string)
	walk = func(parent grpcproc.PID, indent string) {
		kids := children[parent]
		for i, p := range kids {
			branch, next := "├── ", "│   "
			if i == len(kids)-1 {
				branch, next = "└── ", "    "
			}
			_, _ = fmt.Fprintf(w, "%s%s%s\t%s\n", indent, branch, p.Name, p.Label)
			walk(p.PID, indent+next)
		}
	}
	walk(grpcproc.PID{}, "")
	_ = w.Flush()
	return b.String()
}

// golden compares got with testdata/name, or rewrites it with -update.
func golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("%s changed; run go test ./guide -update\n%s", path, got)
	}
}
