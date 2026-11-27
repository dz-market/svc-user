package handler_test

import (
	"errors"
	"log/slog"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	autheventsv1 "github.com/dz-market/protobuf/gen/go/auth/events/v1"

	"github.com/dz-market/svc-user/internal/application/user"
	"github.com/dz-market/svc-user/internal/delivery/event/kafka/consumer"
	"github.com/dz-market/svc-user/internal/delivery/event/kafka/handler"
	"github.com/dz-market/svc-user/internal/delivery/event/kafka/handler/mocks"
)

func TestUserRegisteredHandle(t *testing.T) {
	t.Parallel()

	input := user.CreateProfileInput{
		UserID:       uuid.New(),
		RegisteredAt: time.Now().UTC(),
	}
	payload := marshalUserRegistered(t, input.UserID.String(), timestamppb.New(input.RegisteredAt))
	errUnexpected := errors.New("unexpected error")

	tests := []struct {
		name    string
		wantErr error
	}{
		{name: "success"},
		{name: "service error", wantErr: errUnexpected},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				t.Parallel()

				svc := mocks.NewMockUserService(t)
				svc.EXPECT().
					CreateProfile(mock.Anything, input).
					Return(tt.wantErr)

				h := handler.NewUserRegistered(svc, slog.New(slog.DiscardHandler))
				err := h.Handle(t.Context(), consumer.Message{Value: payload})

				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)

					return
				}

				require.NoError(t, err)
			},
		)
	}
}

func TestUserRegisteredSkipsMalformedEvents(t *testing.T) {
	t.Parallel()

	userID := uuid.New().String()
	registeredAt := timestamppb.Now()

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "invalid protobuf", payload: []byte{0xff}},
		{name: "invalid user ID", payload: marshalUserRegistered(t, "invalid", registeredAt)},
		{name: "missing timestamp", payload: marshalUserRegistered(t, userID, nil)},
		{name: "invalid timestamp", payload: marshalUserRegistered(t, userID, &timestamppb.Timestamp{Nanos: -1})},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				t.Parallel()

				svc := mocks.NewMockUserService(t)
				h := handler.NewUserRegistered(svc, slog.New(slog.DiscardHandler))
				err := h.Handle(t.Context(), consumer.Message{Value: tt.payload})

				require.NoError(t, err)
			},
		)
	}
}

func marshalUserRegistered(t *testing.T, userID string, registeredAt *timestamppb.Timestamp) []byte {
	t.Helper()

	payload, err := proto.Marshal(
		autheventsv1.UserRegistered_builder{
			UserId:       new(userID),
			RegisteredAt: registeredAt,
		}.Build(),
	)
	require.NoError(t, err)

	return payload
}
