package server

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/reflection"

	pserver "github.com/dz-market/platform/grpc/server"
)

type Options struct {
	Addr            string
	Reflection      bool
	ShutdownTimeout time.Duration
}

type Server struct {
	grpc            *grpc.Server
	health          *health.Server
	addr            string
	shutdownTimeout time.Duration
	log             *slog.Logger
}

func New(opts Options, log *slog.Logger) *Server {
	srv := grpc.NewServer()

	healthSrv := pserver.RegisterHealth(srv)

	if opts.Reflection {
		reflection.Register(srv)

		log.Warn("grpc reflection is enabled")
	}

	return &Server{
		grpc:            srv,
		health:          healthSrv,
		addr:            opts.Addr,
		shutdownTimeout: opts.ShutdownTimeout,
		log:             log,
	}
}

func (s *Server) Run(ctx context.Context) error {
	return pserver.ListenAndServe(
		ctx, s.grpc, s.addr, pserver.ServeOptions{
			ShutdownTimeout: s.shutdownTimeout,
			BeforeStop:      s.health.Shutdown,
		}, s.log,
	)
}

func (s *Server) Registrar() grpc.ServiceRegistrar {
	return s.grpc
}
