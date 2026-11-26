package bootstrap

import (
	"context"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	phealth "github.com/dz-market/platform/grpc/health"

	"github.com/dz-market/svc-user/internal/delivery/event/kafka/consumer"
	"github.com/dz-market/svc-user/internal/delivery/grpc/server"
)

type app struct {
	grpcServer    *server.Server
	checker       *phealth.Checker
	kafkaConsumer *consumer.Consumer
	healthTimeout time.Duration
	log           *slog.Logger
}

func (a *app) run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(
		func() error {
			return a.checker.Run(ctx)
		},
	)

	g.Go(
		func() error {
			return a.grpcServer.Run(
				ctx, func() {
					shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.healthTimeout)
					defer cancel()

					if err := a.checker.Shutdown(shutdownCtx); err != nil {
						a.log.WarnContext(shutdownCtx, "grpc health shutdown failed", slog.Any("err", err))
					}
				},
			)
		},
	)

	g.Go(
		func() error {
			return a.kafkaConsumer.Run(ctx)
		},
	)

	if err := g.Wait(); err != nil {
		return err
	}

	a.log.InfoContext(ctx, "service stopped")

	return nil
}
