// Package web is the shop's HTTP front. A handler is not a process: it
// calls the orders desk through the node, as any code outside a process
// does, and the desk may run on this node or another.
package web

import (
	"context"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"golang.yandex/di"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
	ordersv1 "github.com/floatdrop/grpcproc/examples/guide/proto/orders/v1"
)

type front struct {
	node *grpcproc.Node
	desk grpcproc.Addr[*ordersv1.Place]
	log  *slog.Logger
}

func newFront(n *grpcproc.Node, cfg platform.Config, log *slog.Logger) *front {
	return &front{node: n, desk: ordersv1.Desk(cfg.Where(ordersv1.Service)), log: log}
}

type order struct {
	Sku  string `json:"sku"`
	Qty  int64  `json:"qty"`
	Card string `json:"card"`
}

type placed struct {
	Order   string `json:"order"`
	Receipt string `json:"receipt"`
	Left    int64  `json:"left"`
}

// place is POST /orders. A client that hangs up stops the wait for the
// desk's answer; the desk still takes the order it was sent.
func (f *front) place(w http.ResponseWriter, r *http.Request) {
	var o order
	if err := json.UnmarshalRead(r.Body, &o); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	p, err := f.node.Call[*ordersv1.Placed](r.Context(), f.desk, &ordersv1.Place{Sku: o.Sku, Qty: o.Qty, Card: o.Card})
	switch {
	case err != nil:
		// No answer: the desk, or a service it needs, is out of reach.
		// What went wrong is for the log, not for the client.
		f.log.Error("order failed", "err", err)
		http.Error(w, "the shop cannot take orders right now", http.StatusServiceUnavailable)
	case p.Refused != "":
		http.Error(w, p.Refused, http.StatusUnprocessableEntity)
	default:
		w.Header().Set("Content-Type", "application/json")
		_ = json.MarshalWrite(w, placed{Order: p.Order, Receipt: p.Receipt, Left: p.Left})
	}
}

func newServer(cfg platform.Config, f *front) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", f.place)
	return &http.Server{Addr: cfg.HTTP, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

// Module registers the front and its server. The server drains before
// anything stops, so an order in flight still finds the desk.
func Module(s *di.Scope) {
	s.Wire[*front](newFront)
	s.Wire[*http.Server](newServer).
		Eager().
		OnStart(func(_ context.Context, srv *http.Server) error {
			ln, err := net.Listen("tcp", srv.Addr)
			if err != nil {
				return err
			}
			go func() {
				if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
					s.Shutdown(err)
				}
			}()
			return nil
		}).
		OnDrain(func(ctx context.Context, srv *http.Server) error { return srv.Shutdown(ctx) }).
		OnStop(func(_ context.Context, srv *http.Server) error { return srv.Close() })
}
