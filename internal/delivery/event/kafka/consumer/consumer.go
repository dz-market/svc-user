package consumer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Handler interface {
	Handle(ctx context.Context, msg Message) error
}

type Options struct {
	Client        *kgo.Client
	MinRetryDelay time.Duration
	MaxRetryDelay time.Duration
	CommitTimeout time.Duration
	Log           *slog.Logger
}

type Consumer struct {
	client        *kgo.Client
	handlers      map[string]Handler
	minRetryDelay time.Duration
	maxRetryDelay time.Duration
	commitTimeout time.Duration
	log           *slog.Logger
}

func New(opts Options) *Consumer {
	return &Consumer{
		client:        opts.Client,
		handlers:      make(map[string]Handler),
		minRetryDelay: opts.MinRetryDelay,
		maxRetryDelay: opts.MaxRetryDelay,
		commitTimeout: opts.CommitTimeout,
		log:           opts.Log,
	}
}

func (c *Consumer) Register(topic string, handler Handler) {
	c.handlers[topic] = handler
}

func (c *Consumer) SetClient(client *kgo.Client) {
	c.client = client
}

func (c *Consumer) Topics() []string {
	topics := make([]string, 0, len(c.handlers))

	for topic := range c.handlers {
		topics = append(topics, topic)
	}

	return topics
}

func (c *Consumer) Run(ctx context.Context) error {
	if c.client == nil {
		return errors.New("kafka client is not set")
	}

	for {
		fetches := c.client.PollFetches(ctx)

		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
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
	handler, ok := c.handlers[rec.Topic]
	if !ok {
		c.log.WarnContext(
			ctx, "kafka message topic has no registered handler",
			slog.String("topic", rec.Topic),
			slog.Int("partition", int(rec.Partition)),
			slog.Int64("offset", rec.Offset),
		)

		return true
	}

	msg := newMessage(rec)

	for attempt := 1; ; attempt++ {
		err := handler.Handle(ctx, msg)
		if err == nil {
			return true
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

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.commitTimeout)
	defer cancel()

	if err := c.client.CommitRecords(ctx, recs...); err != nil {
		c.log.ErrorContext(
			ctx, "kafka commit failed",
			slog.Any("err", err),
		)
	}
}

func (c *Consumer) retryDelay(attempt int) time.Duration {
	return min(c.minRetryDelay<<min(attempt-1, 20), c.maxRetryDelay)
}
