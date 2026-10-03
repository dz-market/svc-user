package handler

import (
	"context"

	"github.com/dz-market/svc-user/internal/application/user"
)

type UserService interface {
	CreateProfile(ctx context.Context, in user.CreateProfileInput) error
}
