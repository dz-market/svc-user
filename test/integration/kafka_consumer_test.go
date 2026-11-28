//go:build integration

package integration

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dz-market/svc-user/internal/delivery/event/kafka/consumer"
	"github.com/dz-market/svc-user/internal/infrastructure/messaging/kafka"
)

func TestKafkaConsumer(t *testing.T) {
	broker := startRedpanda(t)

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.AllowAutoTopicCreation(),
	)
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	errHandler := errors.New("handler failed")

	tests := []struct {
		name    string
		wantErr error
	}{
		{name: "success"},
		{name: "error", wantErr: errHandler},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
				defer cancel()

				topic := "consumer" + tt.name
				record := &kgo.Record{
					Topic: topic,
					Value: []byte("event"),
				}
				require.NoError(t, producer.ProduceSync(ctx, record).FirstErr())

				handler := &testHandler{
					err:  tt.wantErr,
					stop: cancel,
				}

				err := runKafkaConsumer(ctx, broker, topic, topic, handler)
				require.ErrorIs(t, err, tt.wantErr)
				require.Equal(t, "event", handler.received)
			},
		)
	}
}

type testHandler struct {
	err      error
	stop     context.CancelFunc
	received string
}

func (h *testHandler) Handle(_ context.Context, msg consumer.Message) error {
	h.received = string(msg.Value)

	if h.err == nil {
		h.stop()

		return nil
	}

	return h.err
}

func startRedpanda(t *testing.T) string {
	t.Helper()

	ctx := t.Context()
	ctr, err := redpanda.Run(
		ctx, "redpandadata/redpanda:v26.2.3",
		redpanda.WithAutoCreateTopics(),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	broker, err := ctr.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	return broker
}

func runKafkaConsumer(ctx context.Context, broker, topic, group string, handler consumer.Handler) error {
	log := slog.New(slog.DiscardHandler)

	client, err := kafka.NewConsumerClient(
		kafka.ConsumerOptions{
			Brokers:  []string{broker},
			ClientID: group,
			GroupID:  group,
			Topics:   []string{topic},
			Log:      log,
		},
	)
	if err != nil {
		return err
	}
	defer client.Close()

	c := consumer.New(
		consumer.Options{
			Client:        client,
			Handlers:      map[string]consumer.Handler{topic: handler},
			MinRetryDelay: 10 * time.Millisecond,
			MaxRetryDelay: 40 * time.Millisecond,
			CommitTimeout: 3 * time.Second,
			BatchTimeout:  5 * time.Second,
			Log:           log,
		},
	)

	return c.Run(ctx)
}
