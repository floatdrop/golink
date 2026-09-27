package guide

import (
	"net/http"
	"strings"
	"testing"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
)

// The local deployment: every service in one process, on one node.
func TestLocal(t *testing.T) {
	app := compose(platform.Config{Node: "shop"}, local)
	start(t, app)

	if code, body := order(t, app, `{"sku":"apple","qty":2,"card":"4242"}`); code != http.StatusOK || !strings.Contains(body, `"left":8`) {
		t.Fatalf("%d %s", code, body)
	}
	// A declined card: the desk says no, and gives back what it reserved.
	if code, body := order(t, app, `{"sku":"apple","qty":3,"card":"4000-0002"}`); code != http.StatusUnprocessableEntity || !strings.Contains(body, "declined") {
		t.Fatalf("%d %s", code, body)
	}
	if code, body := order(t, app, `{"sku":"apple","qty":1,"card":"4242"}`); code != http.StatusOK || !strings.Contains(body, `"left":7`) {
		t.Fatalf("%d %s", code, body)
	}
	golden(t, "local.txt", tree(app.Get[*grpcproc.Node]()))
}
