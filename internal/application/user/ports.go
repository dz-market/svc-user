package user

import (
	"context"
	"uuid"

	"github.com/dz-market/svc-user/internal/domain/profile"
)

type ProfileRepository interface {
	Create(ctx context.Context, p profile.Profile) error
	ByUserID(ctx context.Context, userID uuid.UUID) (profile.Profile, error)
}
