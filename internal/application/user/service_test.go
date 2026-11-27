package user_test

import (
	"context"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/dz-market/svc-user/internal/application/user"
	"github.com/dz-market/svc-user/internal/application/user/mocks"
	"github.com/dz-market/svc-user/internal/domain/profile"
)

func TestCreateProfile(t *testing.T) {
	t.Parallel()

	input := user.CreateProfileInput{
		UserID:       uuid.New(),
		RegisteredAt: time.Now(),
	}
	wantProfile := profile.Profile{
		UserID:    input.UserID,
		CreatedAt: input.RegisteredAt,
	}

	tests := []struct {
		name    string
		wantErr error
	}{
		{name: "success"},
		{name: "repository error", wantErr: context.DeadlineExceeded},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				t.Parallel()

				repo := mocks.NewMockProfileRepository(t)
				repo.EXPECT().
					Create(mock.Anything, wantProfile).
					Return(tt.wantErr)

				svc := user.NewService(user.Options{ProfileRepo: repo})
				err := svc.CreateProfile(t.Context(), input)

				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)

					return
				}

				require.NoError(t, err)
			},
		)
	}
}

func TestGetMe(t *testing.T) {
	t.Parallel()

	savedProfile := profile.Profile{
		UserID:    uuid.New(),
		CreatedAt: time.Now(),
	}
	input := user.GetMeInput{UserID: savedProfile.UserID}

	tests := []struct {
		name        string
		wantProfile profile.Profile
		wantErr     error
	}{
		{name: "success", wantProfile: savedProfile},
		{name: "not found", wantErr: profile.ErrNotFound},
		{name: "repository error", wantErr: context.DeadlineExceeded},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				t.Parallel()

				repo := mocks.NewMockProfileRepository(t)
				repo.EXPECT().
					ByUserID(mock.Anything, input.UserID).
					Return(tt.wantProfile, tt.wantErr)

				svc := user.NewService(user.Options{ProfileRepo: repo})
				out, err := svc.GetMe(t.Context(), input)

				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)

					return
				}

				require.NoError(t, err)
				require.Equal(t, tt.wantProfile, out.Profile)
			},
		)
	}
}
