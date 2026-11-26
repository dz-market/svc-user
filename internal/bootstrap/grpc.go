package bootstrap

import (
	"context"
	"fmt"
	"log/slog"

	"buf.build/go/protovalidate"

	phealth "github.com/dz-market/platform/grpc/health"
	userv1 "github.com/dz-market/protobuf/gen/go/user/api/v1"

	"github.com/dz-market/svc-user/internal/config"
	"github.com/dz-market/svc-user/internal/delivery/grpc/handler"
	"github.com/dz-market/svc-user/internal/delivery/grpc/server"
	"github.com/dz-market/svc-user/internal/delivery/grpc/server/interceptor"
)

func buildGRPC(
	cfg config.Config, userService handler.UserService, verifier interceptor.TokenVerifier,
	pingPostgres func(context.Context) error, log *slog.Logger,
) (*server.Server, *phealth.Checker, error) {
	validator, err := protovalidate.New()
	if err != nil {
		return nil, nil, fmt.Errorf("protovalidate: %w", err)
	}

	srv := server.New(
		server.Options{
			Addr:                  cfg.GRPC.Addr,
			Reflection:            cfg.GRPC.Reflection,
			MaxRecvMsgSize:        cfg.GRPC.MaxRecvMsgSize.Bytes(),
			MaxConnectionAge:      cfg.GRPC.Keepalive.MaxConnectionAge,
			MaxConnectionAgeGrace: cfg.GRPC.Keepalive.MaxConnectionAgeGrace,
			ShutdownTimeout:       cfg.GRPC.ShutdownTimeout,
			Validator:             validator,
			Verifier:              verifier,
		},
		log,
	)

	userHandler := handler.NewUser(
		handler.Options{
			Service: userService,
			Log:     log,
		},
	)

	userv1.RegisterUserServiceServer(srv.Registrar(), userHandler)

	checker, err := phealth.New(
		srv.Registrar(), phealth.Options{
			Period:  cfg.Health.Period,
			Timeout: cfg.Health.Timeout,
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create grpc health checker: %w", err)
	}

	if err := checker.Register("postgres", phealth.DependencyOptions{}, pingPostgres); err != nil {
		return nil, nil, fmt.Errorf("register postgres health check: %w", err)
	}

	return srv, checker, nil
}
