//go:build integration

package integration

import (
	"os"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/dz-market/svc-user/internal/domain/profile"
	"github.com/dz-market/svc-user/internal/infrastructure/persistence/postgres"
)

func TestProfileRepository(t *testing.T) {
	repo := postgres.NewProfileRepository(startPostgres(t))
	createdAt := time.Now().UTC().Truncate(time.Microsecond)

	t.Run(
		"create and find profile by ID", func(t *testing.T) {
			want := profile.Profile{
				UserID:    uuid.New(),
				CreatedAt: createdAt,
			}
			require.NoError(t, repo.Create(t.Context(), want))

			got, err := repo.ByUserID(t.Context(), want.UserID)
			require.NoError(t, err)
			require.Equal(t, want.UserID, got.UserID)
			require.Equal(t, want.CreatedAt, got.CreatedAt.UTC())
		},
	)

	t.Run(
		"not found", func(t *testing.T) {
			_, err := repo.ByUserID(t.Context(), uuid.New())
			require.ErrorIs(t, err, profile.ErrNotFound)
		},
	)

	t.Run(
		"duplicate keeps the original profile", func(t *testing.T) {
			want := profile.Profile{
				UserID:    uuid.New(),
				CreatedAt: createdAt,
			}
			require.NoError(t, repo.Create(t.Context(), want))
			require.NoError(
				t, repo.Create(
					t.Context(), profile.Profile{
						UserID:    want.UserID,
						CreatedAt: want.CreatedAt.Add(time.Hour),
					},
				),
			)

			got, err := repo.ByUserID(t.Context(), want.UserID)
			require.NoError(t, err)
			require.Equal(t, want.CreatedAt, got.CreatedAt.UTC())
		},
	)
}

func startPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx := t.Context()
	ctr, err := tcpostgres.Run(
		ctx, "postgres:18-alpine",
		tcpostgres.WithDatabase("svc_user"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	db := stdlib.OpenDBFromPool(pool)

	t.Cleanup(
		func() {
			require.NoError(t, db.Close())
		},
	)

	migrations, err := goose.NewProvider(goose.DialectPostgres, db, os.DirFS("../../migrations"))
	require.NoError(t, err)

	_, err = migrations.Up(ctx)
	require.NoError(t, err)

	return pool
}
