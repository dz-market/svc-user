package config

import (
	"context"

	pconfig "github.com/dz-market/platform/config"
)

func Load(ctx context.Context) (Config, error) {
	return pconfig.Load[Config](ctx)
}
