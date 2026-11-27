package handler_test

import (
	"errors"
	"log/slog"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	userv1 "github.com/dz-market/protobuf/gen/go/user/api/v1"

	"github.com/dz-market/svc-user/internal/application/user"
	"github.com/dz-market/svc-user/internal/delivery/grpc/handler"
	"github.com/dz-market/svc-user/internal/delivery/grpc/handler/mocks"
	"github.com/dz-market/svc-user/internal/delivery/grpc/identity"
	"github.com/dz-market/svc-user/internal/domain/profile"
)

func TestGetMe(t *testing.T) {
	t.Parallel()

	savedProfile := profile.Profile{
		UserID:    uuid.New(),
		CreatedAt: time.Now().UTC(),
	}
	input := user.GetMeInput{UserID: savedProfile.UserID}
	errUnexpected := errors.New("unexpected error")

	tests := []struct {
		name        string
		wantProfile profile.Profile
		wantErr     error
		wantCode    codes.Code
	}{
		{name: "success", wantProfile: savedProfile, wantCode: codes.OK},
		{name: "not found", wantErr: profile.ErrNotFound, wantCode: codes.NotFound},
		{name: "unexpected error", wantErr: errUnexpected, wantCode: codes.Internal},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				t.Parallel()

				svc := mocks.NewMockUserService(t)
				svc.EXPECT().
					GetMe(mock.Anything, input).
					Return(user.GetMeOutput{Profile: tt.wantProfile}, tt.wantErr)

				h := handler.NewUser(
					handler.Options{
						Service: svc,
						Log:     slog.New(slog.DiscardHandler),
					},
				)
				ctx := identity.WithUserID(t.Context(), input.UserID)

				out, err := h.GetMe(ctx, &userv1.GetMeRequest{})
				require.Equal(t, tt.wantCode, status.Code(err))

				if tt.wantCode != codes.OK {
					require.Nil(t, out)

					return
				}

				got := out.GetProfile()
				require.Equal(t, tt.wantProfile.UserID.String(), got.GetUserId())
				require.Equal(t, tt.wantProfile.CreatedAt, got.GetCreatedAt().AsTime())
			},
		)
	}
}

func TestGetMeWithoutIdentity(t *testing.T) {
	t.Parallel()

	h := handler.NewUser(
		handler.Options{
			Service: mocks.NewMockUserService(t),
			Log:     slog.New(slog.DiscardHandler),
		},
	)

	out, err := h.GetMe(t.Context(), &userv1.GetMeRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.Nil(t, out)
}
