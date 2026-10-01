package handler

import (
	"context"
	"time"
	"uuid"
)

type ProfileRepository interface {
	Create(ctx context.Context, userID uuid.UUID, registeredAt time.Time) error
}
