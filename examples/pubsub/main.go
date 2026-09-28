// Pub/sub: a process publishes what changes to a topic, and whoever wants
// to know subscribes, on any node, without the publisher knowing who they
// are. Two nodes, in one binary, on loopback.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/shoppb"
	"github.com/floatdrop/grpcproc/pubsub"
)

// inventory reserves stock, and publishes the level after each reservation
// to a topic it owns. The topic keeps the last two levels for whoever
// subscribes later, and ends when inventory does.
func inventory(p *grpcproc.Process[*shoppb.Reserve]) error {
	stock, err := pubsub.SpawnOwned[*shoppb.Reserved](p, pubsub.Config{Buffer: 2}, grpcproc.WithName("stock"))
	if err != nil {
		return err
	}
	left := map[string]int64{"apple": 10}
	for {
		m, err := p.Receive()
		if err != nil {
			return err
		}
		if left[m.Body.Sku] < m.Body.Qty {
			_ = m.Reply(nil, fmt.Errorf("only %d %s left", left[m.Body.Sku], m.Body.Sku))
			continue
		}
		left[m.Body.Sku] -= m.Body.Qty
		level := &shoppb.Reserved{Sku: m.Body.Sku, Left: left[m.Body.Sku]}
		_ = stock.Publish(p.Context(), p, level)
		_ = m.Reply(level, nil)
	}
}

// dashboard subscribes to the stock topic of another node and prints each
// level until the topic ends. Its mailbox holds the topic's events, so they
// are plain messages to it.
func dashboard(subscribed, done chan<- struct{}) func(*grpcproc.Process[*shoppb.Reserved]) error {
	return func(p *grpcproc.Process[*shoppb.Reserved]) error {
		defer close(done)
		ctx, cancel := context.WithTimeout(p.Context(), 5*time.Second)
		defer cancel()
		sub, err := pubsub.Named[*shoppb.Reserved]("warehouse", "stock").Subscribe(ctx, p)
		if err != nil {
			return err
		}
		close(subscribed)
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.Down != nil && m.Down.Ref == sub.Ref {
				fmt.Println("stock ended:", m.Down.Reason)
				return nil
			}
			fmt.Printf("%s left: %d\n", m.Body.Sku, m.Body.Left)
		}
	}
}

func main() {
	ctx := context.Background()
	warehouse, shop := node(ctx, "warehouse"), node(ctx, "shop")

	inv, err := warehouse.Spawn(inventory, grpcproc.WithName("inventory"))
	check(err)
	reserve := grpcproc.Named[*shoppb.Reserve]("warehouse", "inventory")

	// Three reservations before anyone listens: the topic keeps the last two.
	for range 3 {
		_, err := reserve.Call[*shoppb.Reserved](ctx, shop, &shoppb.Reserve{Sku: "apple", Qty: 1})
		check(err)
	}

	// The dashboard subscribes from the other node, and gets those two first.
	subscribed, done := make(chan struct{}), make(chan struct{})
	_, err = shop.Spawn(dashboard(subscribed, done))
	check(err)
	<-subscribed

	// What is published from now on reaches it as it happens.
	_, err = reserve.Call[*shoppb.Reserved](ctx, shop, &shoppb.Reserve{Sku: "apple", Qty: 2})
	check(err)

	// The topic is inventory's: when inventory exits, the dashboard learns
	// that the topic ended, and why.
	check(warehouse.Exit(ctx, inv, "closing"))
	<-done

	check(shop.Stop(ctx))
	check(warehouse.Stop(ctx))
}

// peers is where each node's gRPC server listens: a static address book
// here, grpcproc/etcd in a real cluster.
var peers = grpcproc.StaticResolver{}

// node runs a node on its own gRPC server.
func node(ctx context.Context, name string) *grpcproc.Node {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	check(err)
	peers[name] = lis.Addr().String()

	n, err := grpcproc.NewNode(grpcproc.Config{
		Name:        name,
		Resolver:    peers,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	check(err)
	srv := grpc.NewServer()
	n.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	check(n.Start(ctx))
	return n
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
