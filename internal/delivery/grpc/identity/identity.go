package identity

import (
	"context"
	"uuid"
)

type identityKey struct{}

func WithUserID(ctx context.Context, userID uuid.UUID) context.Context {
	return context.WithValue(ctx, identityKey{}, userID)
}

func UserID(ctx context.Context) (uuid.UUID, bool) {
	id, ok := ctx.Value(identityKey{}).(uuid.UUID)

	return id, ok
}
