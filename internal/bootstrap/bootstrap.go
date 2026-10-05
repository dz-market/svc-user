package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"syscall"

	"buf.build/go/protovalidate"
	"golang.org/x/sync/errgroup"

	"github.com/dz-market/platform/health"
	userv1 "github.com/dz-market/protobuf/gen/go/user/api/v1"

	"github.com/dz-market/svc-user/internal/application/user"
	"github.com/dz-market/svc-user/internal/config"
	"github.com/dz-market/svc-user/internal/delivery/event/kafka/consumer"
	kafkahandler "github.com/dz-market/svc-user/internal/delivery/event/kafka/handler"
	"github.com/dz-market/svc-user/internal/delivery/grpc/handler"
	"github.com/dz-market/svc-user/internal/delivery/grpc/server"
	"github.com/dz-market/svc-user/internal/infrastructure/client/auth"
	"github.com/dz-market/svc-user/internal/infrastructure/messaging/kafka"
	logger "github.com/dz-market/svc-user/internal/infrastructure/observability/logger/slog"
	"github.com/dz-market/svc-user/internal/infrastructure/persistence/postgres"
	"github.com/dz-market/svc-user/internal/infrastructure/security/jwt"
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

	db, err := postgres.New(
		ctx, postgres.Options{
			AppName:           cfg.ServiceName,
			DSN:               cfg.Postgres.DSN,
			MaxConns:          cfg.Postgres.MaxConns,
			MinConns:          cfg.Postgres.MinConns,
			MaxConnLifetime:   cfg.Postgres.MaxConnLifetime,
			MaxConnIdleTime:   cfg.Postgres.MaxConnIdleTime,
			HealthCheckPeriod: cfg.Postgres.HealthCheckPeriod,
			ConnectTimeout:    cfg.Postgres.ConnectTimeout,
			PingTimeout:       cfg.Postgres.PingTimeout,
		}, log,
	)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}

	defer db.Close()

	authClient, err := auth.New(
		auth.Options{
			Addr: cfg.AuthService.Addr,
		},
	)
	if err != nil {
		return fmt.Errorf("auth client: %w", err)
	}

	defer func() {
		if err := authClient.Close(); err != nil {
			log.ErrorContext(
				ctx, "close auth client",
				slog.Any("err", err),
			)

			return
		}

		log.InfoContext(ctx, "auth client closed")
	}()

	publicKey, err := authClient.GetPublicKey(ctx)
	if err != nil {
		return fmt.Errorf("get auth public key: %w", err)
	}

	verifier := jwt.NewVerifier(publicKey)

	profileRepo := postgres.NewProfileRepository(db)

	userService := user.NewService(
		user.Options{
			ProfileRepo: profileRepo,
		},
	)

	handlers := map[string]consumer.Handler{
		cfg.Kafka.Topics.UserRegistered: kafkahandler.NewUserRegistered(userService, log),
	}

	kafkaConsumerClient, err := kafka.NewConsumerClient(
		kafka.ConsumerOptions{
			Brokers:  cfg.Kafka.Brokers,
			ClientID: cfg.ServiceName,
			GroupID:  cfg.ServiceName,
			Topics:   slices.Collect(maps.Keys(handlers)),
			Log:      log,
		},
	)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}

	kafkaConsumer := consumer.New(
		consumer.Options{
			Client:   kafkaConsumerClient,
			Handlers: handlers,
			Log:      log,
		},
	)

	defer kafkaConsumerClient.Close()

	checker := health.New(
		health.Options{
			Period:  cfg.Health.Period,
			Timeout: cfg.Health.Timeout,
		}, log,
	)
	checker.Register("postgres", db.Ping)
	checker.Register(
		"kafka", func(ctx context.Context) error {
			return kafkaConsumerClient.Ping(ctx)
		},
	)

	validator, err := protovalidate.New()
	if err != nil {
		return fmt.Errorf("create protovalidate validator: %w", err)
	}

	srv := server.New(
		server.Options{
			Addr:                  cfg.GRPC.Addr,
			Reflection:            cfg.GRPC.Reflection,
			MaxRecvMsgSize:        cfg.GRPC.MaxRecvMsgSize.Bytes(),
			MaxConnectionAge:      cfg.GRPC.Keepalive.MaxConnectionAge,
			MaxConnectionAgeGrace: cfg.GRPC.Keepalive.MaxConnectionAgeGrace,
			ShutdownTimeout:       cfg.ShutdownTimeout,
			Validator:             validator,
			Verifier:              verifier,
		},
		log,
	)

	userv1.RegisterUserServiceServer(
		srv.Registrar(), handler.NewUser(
			handler.Options{
				Service: userService,
				Log:     log,
			},
		),
	)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(
		func() error {
			return srv.Run(ctx)
		},
	)

	g.Go(
		func() error {
			return kafkaConsumer.Run(ctx)
		},
	)

	if err := g.Wait(); err != nil {
		return err
	}

	log.InfoContext(ctx, "service stopped")

	return nil
}
