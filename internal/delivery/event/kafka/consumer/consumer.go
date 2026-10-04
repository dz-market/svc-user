package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	minRetryDelay = 1 * time.Second
	maxRetryDelay = 30 * time.Second
	commitTimeout = 10 * time.Second
)

type Handler interface {
	Handle(ctx context.Context, msg Message) error
}

type Options struct {
	Client   *kgo.Client
	Handlers map[string]Handler
	Log      *slog.Logger
}

type Consumer struct {
	client   *kgo.Client
	handlers map[string]Handler
	log      *slog.Logger
}

func New(opts Options) *Consumer {
	return &Consumer{
		client:   opts.Client,
		handlers: opts.Handlers,
		log:      opts.Log,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(ctx)

		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil //nolint:nilerr // shutdown is not an error
		}

		fetches.EachError(
			func(topic string, partition int32, err error) {
				c.log.ErrorContext(
					ctx, "kafka fetch failed",
					slog.String("topic", topic),
					slog.Int("partition", int(partition)),
					slog.Any("err", err),
				)
			},
		)

		if fetches.Empty() {
			continue
		}

		handled := make([]*kgo.Record, 0, fetches.NumRecords())

		for iter := fetches.RecordIter(); !iter.Done(); {
			rec := iter.Next()

			if !c.handle(ctx, rec) {
				break
			}

			handled = append(handled, rec)
		}

		c.commit(ctx, handled)
	}
}

func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) bool {
	handler := c.handlers[rec.Topic]

	msg := newMessage(rec)

	for attempt := 1; ; attempt++ {
		err := handler.Handle(ctx, msg)
		if err == nil {
			return true
		}

		if ctx.Err() != nil {
			return false
		}

		delay := c.retryDelay(attempt)

		c.log.WarnContext(
			ctx, "kafka message not handled, retrying",
			slog.String("topic", msg.Topic),
			slog.Int("partition", int(msg.Partition)),
			slog.Int64("offset", msg.Offset),
			slog.Int("attempt", attempt),
			slog.Duration("delay", delay),
			slog.Any("err", err),
		)

		select {
		case <-ctx.Done():
			return false
		case <-time.After(delay):
		}
	}
}

func (c *Consumer) commit(ctx context.Context, recs []*kgo.Record) {
	if len(recs) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer cancel()

	if err := c.client.CommitRecords(ctx, recs...); err != nil {
		c.log.ErrorContext(
			ctx, "kafka commit failed",
			slog.Any("err", err),
		)
	}
}

func (c *Consumer) retryDelay(attempt int) time.Duration {
	return min(minRetryDelay<<min(attempt-1, 20), maxRetryDelay)
}
