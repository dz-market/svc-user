package profile

import (
	"time"
	"uuid"
)

type Profile struct {
	UserID   uuid.UUID
	CreateAt time.Time
}
