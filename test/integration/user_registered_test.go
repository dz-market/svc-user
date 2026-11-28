//go:build integration

package integration

import (
	"context"
	"log/slog"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	autheventsv1 "github.com/dz-market/protobuf/gen/go/auth/events/v1"

	"github.com/dz-market/svc-user/internal/application/user"
	"github.com/dz-market/svc-user/internal/delivery/event/kafka/handler"
	"github.com/dz-market/svc-user/internal/infrastructure/persistence/postgres"
)

func TestUserRegistered(t *testing.T) {
	const topic = "auth.user-registered.v1"

	pool := startPostgres(t)
	broker := startRedpanda(t)

	repo := postgres.NewProfileRepository(pool)
	svc := user.NewService(user.Options{ProfileRepo: repo})
	h := handler.NewUserRegistered(svc, slog.New(slog.DiscardHandler))

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.AllowAutoTopicCreation(),
	)
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	userID := uuid.New()
	registeredAt := time.Now().UTC().Truncate(time.Second)

	payload, err := proto.Marshal(
		autheventsv1.UserRegistered_builder{
			EventId:      new(uuid.New().String()),
			UserId:       new(userID.String()),
			RegisteredAt: timestamppb.New(registeredAt),
		}.Build(),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(userID.String()),
		Value: payload,
	}
	require.NoError(t, producer.ProduceSync(ctx, record).FirstErr())

	var worker errgroup.Group

	worker.Go(
		func() error {
			return runKafkaConsumer(ctx, broker, topic, "user-registered", h)
		},
	)
	t.Cleanup(
		func() {
			require.NoError(t, worker.Wait())
		},
	)

	require.Eventually(
		t, func() bool {
			queryCtx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()

			_, err := repo.ByUserID(queryCtx, userID)

			return err == nil
		}, 15*time.Second, 100*time.Millisecond,
	)

	got, err := repo.ByUserID(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, userID, got.UserID)
	require.Equal(t, registeredAt, got.CreatedAt.UTC())
}
