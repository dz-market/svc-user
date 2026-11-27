package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	maxHandlerAttempts = 5
	maxPollRecords     = 50
)

type Handler interface {
	Handle(ctx context.Context, msg Message) error
}

type Options struct {
	Client        *kgo.Client
	Handlers      map[string]Handler
	MinRetryDelay time.Duration
	MaxRetryDelay time.Duration
	CommitTimeout time.Duration
	BatchTimeout  time.Duration
	Log           *slog.Logger
}

type Consumer struct {
	client        *kgo.Client
	handlers      map[string]Handler
	minRetryDelay time.Duration
	maxRetryDelay time.Duration
	commitTimeout time.Duration
	batchTimeout  time.Duration
	log           *slog.Logger
}

func New(opts Options) *Consumer {
	return &Consumer{
		client:        opts.Client,
		handlers:      opts.Handlers,
		minRetryDelay: opts.MinRetryDelay,
		maxRetryDelay: opts.MaxRetryDelay,
		commitTimeout: opts.CommitTimeout,
		batchTimeout:  opts.BatchTimeout,
		log:           opts.Log,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		stopped, err := c.pollBatch(ctx)
		if stopped || err != nil {
			return err
		}
	}
}

func (c *Consumer) pollBatch(ctx context.Context) (bool, error) {
	fetches := c.client.PollRecords(ctx, maxPollRecords)
	defer c.client.AllowRebalance()

	if ctx.Err() != nil || fetches.IsClientClosed() {
		return true, nil
	}

	batchCtx, cancel := context.WithTimeout(ctx, c.batchTimeout)
	defer cancel()

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

	handled := make([]*kgo.Record, 0, fetches.NumRecords())

	var handleErr error

	for iter := fetches.RecordIter(); !iter.Done(); {
		rec := iter.Next()

		if err := c.handle(batchCtx, rec); err != nil {
			handleErr = fmt.Errorf(
				"handle kafka record %s[%d] at offset %d: %w",
				rec.Topic, rec.Partition, rec.Offset, err,
			)

			break
		}

		handled = append(handled, rec)
	}

	commitErr := c.commit(ctx, handled)

	if ctxErr := ctx.Err(); ctxErr != nil && (handleErr == nil || errors.Is(handleErr, ctxErr)) {
		return true, commitErr
	}

	return false, errors.Join(handleErr, commitErr)
}

func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) error {
	handler := c.handlers[rec.Topic]

	msg := newMessage(rec)

	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := handler.Handle(ctx, msg)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if attempt == maxHandlerAttempts {
			return err
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
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (c *Consumer) commit(ctx context.Context, recs []*kgo.Record) error {
	if len(recs) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.commitTimeout)
	defer cancel()

	if err := c.client.CommitRecords(ctx, recs...); err != nil {
		return fmt.Errorf("commit kafka offsets: %w", err)
	}

	return nil
}

func (c *Consumer) retryDelay(attempt int) time.Duration {
	return min(c.minRetryDelay<<min(attempt-1, 20), c.maxRetryDelay)
}
