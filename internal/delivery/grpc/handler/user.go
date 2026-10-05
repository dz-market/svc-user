package handler

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	userv1 "github.com/dz-market/protobuf/gen/go/user/api/v1"

	"github.com/dz-market/svc-user/internal/application/user"
	"github.com/dz-market/svc-user/internal/delivery/grpc/identity"
	"github.com/dz-market/svc-user/internal/delivery/grpc/mapper"
	"github.com/dz-market/svc-user/internal/domain/profile"
)

type Options struct {
	Service UserService
	Log     *slog.Logger
}

type User struct {
	userv1.UnimplementedUserServiceServer

	service UserService
	log     *slog.Logger
}

func NewUser(opts Options) *User {
	return &User{
		service: opts.Service,
		log:     opts.Log,
	}
}

func (h *User) GetMe(ctx context.Context, _ *userv1.GetMeRequest) (*userv1.GetMeResponse, error) {
	userID, ok := identity.UserID(ctx)
	if !ok {
		h.log.ErrorContext(ctx, "identity is missing from the context")

		return nil, status.Error(codes.Unauthenticated, "unauthenticated")
	}

	out, err := h.service.GetMe(
		ctx, user.GetMeInput{
			UserID: userID,
		},
	)
	if err != nil {
		return nil, h.toStatus(ctx, err)
	}

	return mapper.ToGetMeResponse(out), nil
}

func (h *User) toStatus(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, profile.ErrNotFound):
		return status.Error(codes.NotFound, "user profile not found")

	default:
		h.log.ErrorContext(
			ctx, "unhandled error",
			slog.Any("err", err),
		)

		return status.Error(codes.Internal, "internal error")
	}
}
