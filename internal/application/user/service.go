package user

import (
	"context"
	"time"
	"uuid"

	"github.com/dz-market/svc-user/internal/domain/profile"
)

type Options struct {
	ProfileRepo ProfileRepository
}

type Service struct {
	profileRepo ProfileRepository
}

func NewService(opts Options) *Service {
	return &Service{
		profileRepo: opts.ProfileRepo,
	}
}

type CreateProfileInput struct {
	UserID       uuid.UUID
	RegisteredAt time.Time
}

func (s *Service) CreateProfile(ctx context.Context, in CreateProfileInput) error {
	return s.profileRepo.Create(
		ctx, profile.Profile{
			UserID:    in.UserID,
			CreatedAt: in.RegisteredAt,
		},
	)
}

type GetMeInput struct {
	UserID uuid.UUID
}

type GetMeOutput struct {
	Profile profile.Profile
}

func (s *Service) GetMe(ctx context.Context, in GetMeInput) (GetMeOutput, error) {
	p, err := s.profileRepo.ByUserID(ctx, in.UserID)
	if err != nil {
		return GetMeOutput{}, err
	}

	return GetMeOutput{
		Profile: p,
	}, nil
}
