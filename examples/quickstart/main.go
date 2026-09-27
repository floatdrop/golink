// Quick start: two nodes, a typed process on one, called and monitored from
// the other. Each node is normally its own service; here both run in one
// binary, on loopback.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/examples/shoppb"
)

func main() {
	ctx := context.Background()

	// Where each node's gRPC server listens: a static address book here,
	// grpcproc/etcd in a real cluster.
	warehouseLis := listen()
	shopLis := listen()
	peers := grpcproc.StaticResolver{
		"warehouse": warehouseLis.Addr().String(),
		"shop":      shopLis.Addr().String(),
	}

	warehouse, stopWarehouse := serve(ctx, "warehouse", warehouseLis, peers)
	defer stopWarehouse()
	shop, stopShop := serve(ctx, "shop", shopLis, peers)
	defer stopShop()

	// A process on warehouse. Its mailbox holds *shoppb.Reserve and
	// nothing else; it answers each call with *shoppb.Reserved.
	_, err := warehouse.Spawn(func(p *grpcproc.Process[*shoppb.Reserve]) error {
		left := map[string]int64{"apple": 3}
		for {
			m, err := p.Receive()
			if err != nil {
				return err // asked to exit, or the node is stopping
			}
			if left[m.Body.Sku] < m.Body.Qty {
				_ = p.Reply(m, nil, fmt.Errorf("only %d %s left", left[m.Body.Sku], m.Body.Sku))
				continue
			}
			left[m.Body.Sku] -= m.Body.Qty
			_ = p.Reply(m, &shoppb.Reserved{Sku: m.Body.Sku, Left: left[m.Body.Sku]}, nil)
		}
	}, grpcproc.WithName("stock"))
	if err != nil {
		log.Fatal(err)
	}

	// From shop, the process is a node name and a process name. The address
	// carries the mailbox type, so the compiler checks what is sent to it.
	stock := grpcproc.Named[*shoppb.Reserve]("warehouse", "stock")
	for range 2 {
		r, err := shop.Call[*shoppb.Reserved](ctx, stock, &shoppb.Reserve{Sku: "apple", Qty: 2})
		if err != nil {
			fmt.Println("reserve failed:", err) // the handler's error, as a *grpcproc.RemoteError
			continue
		}
		fmt.Println("reserved, left:", r.Left)
	}

	// A process on shop monitors stock, then asks it to exit. The Down
	// arrives with the reason, as it would for a crash or a lost node. It
	// expects no messages, so its mailbox is untyped: proto.Message.
	done := make(chan grpcproc.Down)
	_, err = shop.Spawn(func(p *grpcproc.Process[proto.Message]) error {
		p.Monitor(stock)
		if err := p.Exit(stock, "closing"); err != nil {
			return err
		}
		for {
			m, err := p.Receive()
			if err != nil {
				return err
			}
			if m.Down != nil {
				done <- *m.Down
				return nil
			}
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("stock exited:", (<-done).Reason)

	_, err = shop.Call[*shoppb.Reserved](ctx, stock, &shoppb.Reserve{Sku: "apple", Qty: 1})
	fmt.Println("no such process:", errors.Is(err, grpcproc.ErrNoProc))
}

func listen() net.Listener {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	return lis
}

// serve runs a node on its own gRPC server, the one the service already has
// for its other APIs.
func serve(ctx context.Context, name string, lis net.Listener, peers grpcproc.Resolver) (*grpcproc.Node, func()) {
	node, err := grpcproc.NewNode(grpcproc.Config{
		Name:        name,
		Resolver:    peers,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err != nil {
		log.Fatal(err)
	}
	srv := grpc.NewServer()
	node.Register(srv) // grpcproc.v1.Node, next to the service's own
	go func() { _ = srv.Serve(lis) }()
	if err := node.Start(ctx); err != nil {
		log.Fatal(err)
	}
	return node, func() {
		if err := node.Stop(ctx); err != nil {
			log.Println(err)
		}
		srv.GracefulStop()
	}
}
