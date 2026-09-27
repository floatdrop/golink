package guide

import (
	"net"
	"net/http"
	"strings"
	"testing"

	"golang.yandex/di"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	inventoryv1 "github.com/floatdrop/grpcproc/examples/guide/proto/inventory/v1"
	paymentsv1 "github.com/floatdrop/grpcproc/examples/guide/proto/payments/v1"
)

// The distributed deployment: three nodes, the same modules split between
// them, and the configuration a deployment would give each: PEERS and a
// PLACEMENT telling the front where the other services run.
func TestCluster(t *testing.T) {
	nodes := []struct {
		name     string
		services []di.Module
		ln       net.Listener
		app      *di.Scope
	}{{name: "warehouse", services: warehouse}, {name: "billing", services: billing}, {name: "front", services: front}}
	// Each node's port is bound first, so every node can be told the others'.
	peers := map[string]string{}
	for i := range nodes {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		nodes[i].ln, peers[nodes[i].name] = ln, ln.Addr().String()
	}
	placement := map[string]string{inventoryv1.Service: "warehouse", paymentsv1.Service: "billing"}
	for i := range nodes {
		n := &nodes[i]
		n.app = compose(platform.Config{Node: n.name, Listen: peers[n.name], Peers: peers, Placement: placement}, n.services)
		n.app.Value(n.ln).Override() // the port bound above
		start(t, n.app)
	}
	shop := nodes[2].app

	if code, body := order(t, shop, `{"sku":"pear","qty":3,"card":"4242"}`); code != http.StatusOK || !strings.Contains(body, `"left":1`) {
		t.Fatalf("%d %s", code, body)
	}
	if code, body := order(t, shop, `{"sku":"pear","qty":1,"card":"4000-0002"}`); code != http.StatusUnprocessableEntity || !strings.Contains(body, "declined") {
		t.Fatalf("%d %s", code, body)
	}
	if code, body := order(t, shop, `{"sku":"pear","qty":1,"card":"4242"}`); code != http.StatusOK || !strings.Contains(body, `"left":0`) {
		t.Fatalf("%d %s", code, body)
	}

	var trees []string
	for _, n := range nodes {
		trees = append(trees, tree(n.app.Get[*grpcproc.Node]()))
	}
	golden(t, "cluster.txt", strings.Join(trees, "\n"))

	// With billing gone, an order gets no answer: the front says so, and
	// does not pass on what went wrong between the nodes.
	if err := nodes[1].app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if code, body := order(t, shop, `{"sku":"apple","qty":1,"card":"4242"}`); code != http.StatusServiceUnavailable || strings.Contains(body, "billing") {
		t.Fatalf("%d %s", code, body)
	}
}
