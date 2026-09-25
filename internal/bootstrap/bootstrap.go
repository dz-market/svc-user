package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/dz-market/svc-user/internal/config"
	"github.com/dz-market/svc-user/internal/delivery/grpc/server"
	logger "github.com/dz-market/svc-user/internal/infrastructure/observability/logger/slog"
)

func Run(ctx context.Context, version string) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := logger.New(
		logger.Options{
			Level:   cfg.Log.Level,
			Format:  cfg.Log.Format,
			Service: cfg.ServiceName,
			Version: version,
		},
	)
	log.DebugContext(ctx, "configuration loaded")

	log.InfoContext(
		ctx, "service starting",
		slog.String("go_version", runtime.Version()),
		slog.Int("pid", os.Getpid()),
		slog.String("grpc_addr", cfg.GRPC.Addr),
		slog.String("log_level", cfg.Log.Level.String()),
	)

	srv := server.New(
		server.Options{
			Addr:            cfg.GRPC.Addr,
			Reflection:      cfg.GRPC.Reflection,
			ShutdownTimeout: cfg.ShutdownTimeout,
		},
		log,
	)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(
		func() error {
			return srv.Run(ctx)
		},
	)

	if err := g.Wait(); err != nil {
		return err
	}

	log.InfoContext(ctx, "service stopped")

	return nil
}
