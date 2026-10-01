package client

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/floatdrop/grpcproc"
	inspectv1 "github.com/floatdrop/grpcproc/proto/grpcproc/inspect/v1"
)

var (
	// ErrNoNames is the error of a node without global names (Config.Names).
	ErrNoNames = errors.New("the node has no global names (Config.Names)")
	// ErrUnlisted is the error of a node whose global names cannot be listed,
	// only looked up one at a time.
	ErrUnlisted = errors.New("the node's global names cannot be listed, only looked up by name")
)

// namesErr says plainly why a node answered a names request as it did.
func namesErr(err error) error {
	switch status.Code(err) {
	case codes.FailedPrecondition:
		return fmt.Errorf("%w: %s", ErrNoNames, status.Convert(err).Message())
	case codes.Unimplemented:
		return fmt.Errorf("%w: %s", ErrUnlisted, status.Convert(err).Message())
	}
	return err
}

// LookupName says who holds a global name, as node's copy of the names has
// it; "" is the node serving the Inspector.
func (c *Client) LookupName(ctx context.Context, node, name string) (NameView, bool, error) {
	resp, err := c.rpc.LookupName(ctx, &inspectv1.LookupNameRequest{Node: node, Name: name})
	if err != nil {
		return NameView{}, false, namesErr(err)
	}
	if !resp.GetFound() {
		return NameView{}, false, nil
	}
	return NameView{Name: name, PID: grpcproc.PIDFromProto(resp.GetPid()).String()}, true, nil
}

// Names lists global names with prefix, ordered by name, at most limit of
// them (the node's default, 1000, when 0).
func (c *Client) Names(ctx context.Context, node, prefix string, limit int) ([]NameView, error) {
	resp, err := c.rpc.ListNames(ctx, &inspectv1.ListNamesRequest{Node: node, Prefix: prefix, Limit: uint32(min(max(limit, 0), 1<<31))})
	if err != nil {
		return nil, namesErr(err)
	}
	out := make([]NameView, 0, len(resp.GetNames()))
	for _, g := range resp.GetNames() {
		out = append(out, NameView{Name: g.GetName(), PID: grpcproc.PIDFromProto(g.GetPid()).String()})
	}
	return out, nil
}
