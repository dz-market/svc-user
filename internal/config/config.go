package config

import (
	"log/slog"
	"time"

	pconfig "github.com/dz-market/platform/config"
)

type Config struct {
	ServiceName     string        `validate:"required" yaml:"service_name"`
	ShutdownTimeout time.Duration `validate:"required" yaml:"shutdown_timeout"`

	GRPC        GRPC        `yaml:"grpc"`
	AuthService AuthService `yaml:"auth_service"`
	Postgres    Postgres    `yaml:"postgres"`
	Kafka       Kafka       `yaml:"kafka"`
	Log         Log         `yaml:"log"`
}

type GRPC struct {
	Addr           string           `validate:"required" yaml:"addr"`
	Reflection     bool             `yaml:"reflection"`
	MaxRecvMsgSize pconfig.ByteSize `validate:"required,minsize=1KB,maxsize=64MB" yaml:"max_recv_msg_size"`

	Keepalive Keepalive `yaml:"keepalive"`
}

type Keepalive struct {
	MaxConnectionAge      time.Duration `validate:"required" yaml:"max_connection_age"`
	MaxConnectionAgeGrace time.Duration `validate:"required" yaml:"max_connection_age_grace"`
}

type AuthService struct {
	Addr string `validate:"required" yaml:"addr"`
}

type Postgres struct {
	DSN               string        `validate:"required"        yaml:"dsn"`
	MaxConns          int32         `yaml:"max_conns"`
	MinConns          int32         `yaml:"min_conns"`
	MaxConnLifetime   time.Duration `yaml:"max_conn_lifetime"`
	MaxConnIdleTime   time.Duration `yaml:"max_conn_idle_time"`
	HealthCheckPeriod time.Duration `yaml:"health_check_period"`
	ConnectTimeout    time.Duration `yaml:"connect_timeout"`
	PingTimeout       time.Duration `yaml:"ping_timeout"`
}

type Kafka struct {
	Brokers []string `validate:"required,min=1,dive,required" yaml:"brokers"`
	Topics  Topics   `yaml:"topics"`
}

type Topics struct {
	UserRegistered string `yaml:"user_registered"`
}

type Log struct {
	Level  slog.Level `yaml:"level"`
	Format string     `validate:"required,oneof=json text" yaml:"format"`
}
