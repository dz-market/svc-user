package handler

import (
	"context"
	"fmt"
	"log/slog"
	"uuid"

	"google.golang.org/protobuf/proto"

	autheventsv1 "github.com/dz-market/protobuf/gen/go/auth/events/v1"

	"github.com/dz-market/svc-user/internal/application/user"
	"github.com/dz-market/svc-user/internal/delivery/event/kafka/consumer"
)

const headerEventID = "event-id"

type UserRegistered struct {
	userService UserService
	log         *slog.Logger
}

func NewUserRegistered(userService UserService, log *slog.Logger) *UserRegistered {
	return &UserRegistered{
		userService: userService,
		log:         log,
	}
}

func (h *UserRegistered) Handle(ctx context.Context, msg consumer.Message) error {
	log := h.log.With(
		slog.String("topic", msg.Topic),
		slog.Int("partition", int(msg.Partition)),
		slog.Int64("offset", msg.Offset),
		slog.String("event_id", msg.Headers[headerEventID]),
	)

	var event autheventsv1.UserRegistered

	if err := proto.Unmarshal(msg.Value, &event); err != nil {
		log.WarnContext(
			ctx, "skip malformed event",
			slog.String("reason", "unmarshal"),
			slog.Any("err", err),
		)

		return nil
	}

	userID, err := uuid.Parse(event.GetUserId())
	if err != nil {
		log.WarnContext(
			ctx, "skip malformed event",
			slog.String("reason", "user_id"),
			slog.String("user_id", event.GetUserId()),
			slog.Any("err", err),
		)

		return nil
	}

	registeredAt := event.GetRegisteredAt()
	if err := registeredAt.CheckValid(); err != nil {
		log.WarnContext(
			ctx, "skip malformed event",
			slog.String("reason", "registered_at"),
			slog.Any("err", err),
		)

		return nil
	}

	if err := h.userService.CreateProfile(
		ctx, user.CreateProfileInput{
			UserID:       userID,
			RegisteredAt: registeredAt.AsTime(),
		},
	); err != nil {
		return fmt.Errorf("create profile: %w", err)
	}

	log.DebugContext(
		ctx, "user registered event handled",
		slog.String("user_id", userID.String()),
	)

	return nil
}
