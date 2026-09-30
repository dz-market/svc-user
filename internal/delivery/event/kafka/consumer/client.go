package consumer

import (
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kslog"
)

type Options struct {
	Brokers  []string
	ClientID string
	Group    string
	Topics   []string
	Log      *slog.Logger
}

func NewClient(opts Options) (*kgo.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ClientID(opts.ClientID),
		kgo.ConsumerGroup(opts.Group),
		kgo.ConsumeTopics(opts.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.WithLogger(kslog.New(opts.Log)),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}

	return cl, nil
}
