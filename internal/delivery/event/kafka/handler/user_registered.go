package handler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"uuid"

	"google.golang.org/protobuf/proto"

	autheventsv1 "github.com/dz-market/protobuf/gen/go/auth/events/v1"

	"github.com/dz-market/svc-user/internal/delivery/event/kafka/consumer"
)

type UserRegistered struct {
	profileRepo ProfileRepository
	log         *slog.Logger
}

func NewUserRegistered(profileRepo ProfileRepository, log *slog.Logger) *UserRegistered {
	return &UserRegistered{
		profileRepo: profileRepo,
		log:         log,
	}
}

func (h *UserRegistered) Handle(ctx context.Context, msg consumer.Message) error {
	var event autheventsv1.UserRegistered

	if err := proto.Unmarshal(msg.Value, &event); err != nil {
		h.log.WarnContext(
			ctx, "failed to unmarshal event",
			slog.String("topic", msg.Topic),
			slog.Int("partition", int(msg.Partition)),
			slog.Int64("offset", msg.Offset),
			slog.Any("err", err),
		)

		return nil
	}

	userID, err := uuid.Parse(event.GetUserId())
	if err != nil {
		h.log.WarnContext(
			ctx, "invalid user_id in event",
			slog.String("topic", msg.Topic),
			slog.Int("partition", int(msg.Partition)),
			slog.Int64("offset", msg.Offset),
			slog.String("user_id", event.GetUserId()),
			slog.Any("err", err),
		)

		return nil
	}

	registeredAt := event.GetRegisteredAt()
	if err := registeredAt.CheckValid(); err != nil {
		h.log.WarnContext(
			ctx, "invalid registered_at timestamp",
			slog.String("topic", msg.Topic),
			slog.Int("partition", int(msg.Partition)),
			slog.Int64("offset", msg.Offset),
			slog.Any("err", err),
		)

		return nil
	}

	if err := h.profileRepo.Create(ctx, userID, registeredAt.AsTime()); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}

		return fmt.Errorf("create profile: %w", err)
	}

	h.log.InfoContext(
		ctx, "user registered event handled",
		slog.String("topic", msg.Topic),
		slog.String("user_id", userID.String()),
		slog.Time("registered_at", registeredAt.AsTime()),
	)

	return nil
}
