// Command migrate applies the event_store schema migrations, the way a
// deployment's migration init container does. It reads the same broker.yaml
// as the publisher and subscriber; the migration credential comes from
// BROKER_POSTGRES_USER and BROKER_POSTGRES_PASSWORD.
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/openshift-hyperfleet/hyperfleet-broker/broker"
	hfl "github.com/openshift-hyperfleet/hyperfleet-logger"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("event_store migration failed: %v", err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(hfl.NewHandler("example-migrate", "v0.0.0"))
	return broker.MigrateEventStore(ctx, logger)
}
