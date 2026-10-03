package postgres

import (
	"context"
	"errors"
	"fmt"
	"uuid"

	"github.com/jackc/pgx/v5"

	ppostgres "github.com/dz-market/platform/database/postgres"

	"github.com/dz-market/svc-user/internal/domain/profile"
)

type ProfileRepository struct {
	q ppostgres.Querier
}

func NewProfileRepository(q ppostgres.Querier) *ProfileRepository {
	return &ProfileRepository{
		q: q,
	}
}

func (r *ProfileRepository) Create(ctx context.Context, p profile.Profile) error {
	const query = `
		INSERT INTO profiles (id, created_at)
		VALUES ($1, $2)
	`

	if _, err := r.q.Exec(ctx, query, p.UserID, p.CreatedAt); err != nil {
		if ppostgres.IsUniqueViolation(err, "") {
			return nil
		}

		return fmt.Errorf("create profile: %w", err)
	}

	return nil
}

func (r *ProfileRepository) ByUserID(ctx context.Context, userID uuid.UUID) (profile.Profile, error) {
	const query = `
		SELECT id, created_at
		FROM profiles
		WHERE id = $1
	`

	var p profile.Profile

	if err := r.q.
		QueryRow(ctx, query, userID).
		Scan(&p.UserID, &p.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return profile.Profile{}, profile.ErrNotFound
		}

		return profile.Profile{}, fmt.Errorf("get profile by user id: %w", err)
	}

	return p, nil
}
