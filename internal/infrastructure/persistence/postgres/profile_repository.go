package postgres

import (
	"context"
	"fmt"
	"time"
	"uuid"

	ppostgres "github.com/dz-market/platform/database/postgres"
)

type ProfileRepository struct {
	q ppostgres.Querier
}

func NewProfileRepository(q ppostgres.Querier) *ProfileRepository {
	return &ProfileRepository{
		q: q,
	}
}

func (r *ProfileRepository) Create(ctx context.Context, userID uuid.UUID, registeredAt time.Time) error {
	const query = `
		INSERT INTO profiles (id, created_at)
		VALUES ($1, $2)
	`

	if _, err := r.q.Exec(ctx, query, userID, registeredAt); err != nil {
		if ppostgres.IsUniqueViolation(err, "") {
			return nil
		}

		return fmt.Errorf("create profile: %w", err)
	}

	return nil
}
