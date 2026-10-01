package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

type ConsumerOptions struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topics   []string
	Log      *slog.Logger
}

func NewConsumerClient(opts ConsumerOptions) (*kgo.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ClientID(opts.ClientID),
		kgo.ConsumerGroup(opts.GroupID),
		kgo.ConsumeTopics(opts.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(
			func(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
				if opts.Log != nil {
					opts.Log.InfoContext(
						ctx, "kafka partitions assigned",
						slog.String("group", opts.GroupID),
						slog.Any("partitions", assigned),
					)
				}
			},
		),
		kgo.OnPartitionsRevoked(
			func(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
				if opts.Log != nil {
					opts.Log.InfoContext(
						ctx, "kafka partitions revoked",
						slog.String("group", opts.GroupID),
						slog.Any("partitions", revoked),
					)
				}
			},
		),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer client: %w", err)
	}

	return cl, nil
}
