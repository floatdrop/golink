package web_test

import (
	"net/http"
	"testing"

	"github.com/floatdrop/grpcproc/tools/client"
	"github.com/floatdrop/grpcproc/tools/internal/testcluster"
	"github.com/floatdrop/grpcproc/tools/web"
)

func TestNames(t *testing.T) {
	f := testcluster.Start(t, "nonames")
	s := serve(t, f, web.Options{})
	var names []client.NameView
	if code := get(t, s, "/api/names?prefix=room:&limit=5", &names); code != http.StatusOK || len(names) != 1 || names[0].PID != f.Talker.String() {
		t.Fatalf("%d %v", code, names)
	}
	if code := get(t, s, "/api/names?node=nonames", nil); code == http.StatusOK {
		t.Fatalf("on nonames: %d", code)
	}
}
