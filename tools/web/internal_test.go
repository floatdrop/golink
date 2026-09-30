package web

import (
	"errors"
	"net/http"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHTTPStatus(t *testing.T) {
	for err, want := range map[error]int{
		badRequest{"bad"}:                          http.StatusBadRequest,
		errors.New("bad pid"):                      http.StatusBadRequest,
		status.Error(codes.NotFound, ""):           http.StatusNotFound,
		status.Error(codes.InvalidArgument, ""):    http.StatusBadRequest,
		status.Error(codes.FailedPrecondition, ""): http.StatusBadRequest,
		status.Error(codes.PermissionDenied, ""):   http.StatusForbidden,
		status.Error(codes.DeadlineExceeded, ""):   http.StatusGatewayTimeout,
		status.Error(codes.Unavailable, ""):        http.StatusBadGateway,
		status.Error(codes.Unknown, ""):            http.StatusConflict,
		status.Error(codes.Internal, ""):           http.StatusInternalServerError,
	} {
		if got := httpStatus(err); got != want {
			t.Errorf("%v: %d, want %d", err, got, want)
		}
	}
}
