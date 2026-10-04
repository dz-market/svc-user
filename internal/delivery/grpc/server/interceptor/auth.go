package interceptor

import (
	"context"
	"log/slog"
	"uuid"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dz-market/platform/logger"

	"github.com/dz-market/svc-user/internal/delivery/grpc/identity"
)

var errInvalidAccessToken = status.Error(codes.Unauthenticated, "invalid access token")

type TokenVerifier interface {
	Verify(token string) (uuid.UUID, error)
}

func Auth(verifier TokenVerifier) grpc.UnaryServerInterceptor {
	return auth.UnaryServerInterceptor(
		func(ctx context.Context) (context.Context, error) {
			token, err := auth.AuthFromMD(ctx, "bearer")
			if err != nil {
				return nil, errInvalidAccessToken
			}

			userID, err := verifier.Verify(token)
			if err != nil {
				return nil, errInvalidAccessToken
			}

			ctx = identity.WithUserID(ctx, userID)
			ctx = logger.With(ctx, slog.String("user_id", userID.String()))

			return ctx, nil
		},
	)
}
