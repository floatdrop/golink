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

// inventory is a process: a goroutine with a mailbox. The mailbox holds
// *shoppb.Reserve and nothing else, and every message is a call, answered
// with a *shoppb.Reserved or an error.
func inventory(p *grpcproc.Process[*shoppb.Reserve]) error {
	left := map[string]int64{"apple": 3}
	for {
		m, err := p.Receive()
		if err != nil {
			return err // asked to exit, or the node is stopping
		}
		if left[m.Body.Sku] < m.Body.Qty {
			_ = m.Reply(nil, fmt.Errorf("only %d %s left", left[m.Body.Sku], m.Body.Sku))
			continue
		}
		left[m.Body.Sku] -= m.Body.Qty
		_ = m.Reply(&shoppb.Reserved{Sku: m.Body.Sku, Left: left[m.Body.Sku]}, nil)
	}
}

func main() {
	ctx := context.Background()

	// Two nodes, each on its own gRPC server; see node() below.
	warehouse, shop := node(ctx, "warehouse"), node(ctx, "shop")

	// The process runs on warehouse, under a name.
	_, err := warehouse.Spawn(inventory, grpcproc.WithName("stock"))
	check(err)

	// From shop it is a node name and a process name. The address carries the
	// mailbox type, so the compiler checks what is sent to it.
	stock := grpcproc.Named[*shoppb.Reserve]("warehouse", "stock")
	for range 2 {
		r, err := stock.Call[*shoppb.Reserved](ctx, shop, &shoppb.Reserve{Sku: "apple", Qty: 2})
		if err != nil {
			fmt.Println("reserve failed:", err) // the handler's error, as a *grpcproc.RemoteError
			continue
		}
		fmt.Println("reserved, left:", r.Left)
	}

	// A monitor across nodes works as one within a node does. A process on
	// shop monitors stock, then asks it to exit; the Down arrives with the
	// reason, as it would for a crash or a lost node.
	exited := make(chan string)
	_, err = shop.Spawn(func(p *grpcproc.Process[proto.Message]) error {
		p.Monitor(stock)
		check(p.Exit(stock, "closing"))
		m, err := p.Receive() // the Down: nothing else is sent to this process
		if err != nil {
			return err
		}
		exited <- m.Down.Reason
		return nil
	})
	check(err)
	fmt.Println("stock exited:", <-exited)

	_, err = stock.Call[*shoppb.Reserved](ctx, shop, &shoppb.Reserve{Sku: "apple", Qty: 1})
	fmt.Println("no such process:", errors.Is(err, grpcproc.ErrNoProc))

	check(shop.Stop(ctx))
	check(warehouse.Stop(ctx))
}

// peers is where each node's gRPC server listens: a static address book
// here, grpcproc/etcd in a real cluster.
var peers = grpcproc.StaticResolver{}

// node runs a node on its own gRPC server, the one a service already has
// for its other APIs.
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
	n.Register(srv) // grpcproc.v1.Node, next to the service's own
	go func() { _ = srv.Serve(lis) }()
	check(n.Start(ctx))
	return n
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
