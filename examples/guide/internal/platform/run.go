package platform

import (
	"context"
	"log/slog"
	"os"
	"time"

	"golang.yandex/di"
	"golang.yandex/di/dislog"
)

// Run is a program's main: it composes the platform with the services'
// modules, checks the graph, and runs until a signal arrives. The entry
// points differ only in the modules they pass.
func Run(services ...di.Module) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg, err := Load()
	if err != nil {
		log.Error("configuration", "err", err)
		os.Exit(2)
	}
	app := di.New()
	// dislog logs what the container does: each constructor and hook, so
	// the program says what it builds, starts and stops, in order.
	app.Observe(dislog.New(log.With("node", cfg.Node)))
	Compose(app, cfg, log, services...)

	// Nothing is built yet; the constructors declared their dependencies,
	// so a service that needs what no module provides fails here.
	if err := app.Validate().Err(); err != nil {
		log.Error("invalid wiring", "err", err)
		os.Exit(1)
	}
	if err := app.Run(context.Background(), di.StopTimeout(10*time.Second)); err != nil {
		log.Error("stopped with failures", "err", err)
		os.Exit(1)
	}
}

// Compose registers what a program is: its configuration and logger, the
// platform, and the services it runs. Tests compose the same way.
func Compose(app *di.Scope, cfg Config, log *slog.Logger, services ...di.Module) {
	app.Value(cfg)
	app.Value(log) // the node adds its name to what it logs
	app.Use(Module)
	app.Use(services...)
}
