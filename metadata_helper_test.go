package golink_test

import (
	"context"

	"google.golang.org/grpc/metadata"
)

func metadataCtx(ctx context.Context, kv ...string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, kv...)
}
