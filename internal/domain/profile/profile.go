package profile

import (
	"time"
	"uuid"
)

type Profile struct {
	UserID    uuid.UUID
	CreatedAt time.Time
}
