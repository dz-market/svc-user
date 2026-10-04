package handler

import (
	"context"

	"github.com/dz-market/svc-user/internal/application/user"
)

type UserService interface {
	GetMe(ctx context.Context, in user.GetMeInput) (user.GetMeOutput, error)
}
