package platform_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"golang.yandex/di"
	"google.golang.org/protobuf/proto"

	"github.com/floatdrop/grpcproc"
	"github.com/floatdrop/grpcproc/actor"
	"github.com/floatdrop/grpcproc/examples/guide/internal/platform"
)

// A service that keeps crashing takes its supervisor past its restart
// limit, then the root past its own; the program stops, with the reason,
// rather than go on serving without its services.
func TestAProgramWhoseServicesGiveUpStops(t *testing.T) {
	crashing := func(s *di.Scope) {
		s.Value(actor.ChildSupervisor("crashing", actor.Spec{Children: []actor.ChildSpec{
			actor.ChildFunc("boom", func(*grpcproc.Process[proto.Message]) error { return errors.New("boom") }),
		}})).Group()
	}
	app := di.New()
	platform.Compose(app, platform.Config{Node: "shop", Listen: "127.0.0.1:0"}, slog.New(slog.DiscardHandler), crashing)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err := app.Run(ctx, di.StopTimeout(5*time.Second))
	if err == nil || !strings.Contains(err.Error(), "root supervisor exited: max restarts") {
		t.Fatalf("got %v", err)
	}
}

func TestLoad(t *testing.T) {
	for _, tc := range []struct {
		env  map[string]string
		want string // an error, or where inventory runs
	}{
		{map[string]string{}, "shop"},
		{map[string]string{"NODE": "front", "PEERS": " warehouse = 127.0.0.1:9102 ", "PLACEMENT": "inventory=warehouse"}, "warehouse"},
		{map[string]string{"PEERS": "warehouse"}, `"warehouse" is not name=value`},
		{map[string]string{"PEERS": "a=1,a=2"}, "names a twice"},
		{map[string]string{"PLACEMENT": "inventory=warehouse"}, "PEERS does not name"},
	} {
		for _, key := range []string{"NODE", "PEERS", "PLACEMENT"} {
			t.Setenv(key, tc.env[key])
		}
		cfg, err := platform.Load()
		got := cfg.Where("inventory")
		if err != nil {
			got = err.Error()
		}
		if !strings.Contains(got, tc.want) {
			t.Errorf("%v: got %q, want %q", tc.env, got, tc.want)
		}
	}
}
