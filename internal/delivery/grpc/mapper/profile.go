package mapper

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	userv1 "github.com/dz-market/protobuf/gen/go/user/api/v1"

	"github.com/dz-market/svc-user/internal/application/user"
	"github.com/dz-market/svc-user/internal/domain/profile"
)

func ToGetMeResponse(out user.GetMeOutput) *userv1.GetMeResponse {
	return userv1.GetMeResponse_builder{
		Profile: ToProfile(out.Profile),
	}.Build()
}

func ToProfile(p profile.Profile) *userv1.Profile {
	return userv1.Profile_builder{
		UserId:    new(p.UserID.String()),
		CreatedAt: timestamppb.New(p.CreatedAt),
	}.Build()
}
