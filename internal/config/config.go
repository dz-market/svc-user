package config

import (
	"log/slog"
	"time"
)

type Config struct {
	ServiceName     string        `validate:"required" yaml:"service_name"`
	ShutdownTimeout time.Duration `validate:"required" yaml:"shutdown_timeout"`

	GRPC   GRPC   `yaml:"grpc"`
	Health Health `yaml:"health"`
	Log    Log    `yaml:"log"`
}

type GRPC struct {
	Addr       string `validate:"required" yaml:"addr"`
	Reflection bool   `yaml:"reflection"`
}

type Health struct {
	Period  time.Duration `validate:"required" yaml:"period"`
	Timeout time.Duration `validate:"required" yaml:"timeout"`
}

type Log struct {
	Level  slog.Level `yaml:"level"`
	Format string     `validate:"required,oneof=json text" yaml:"format"`
}
