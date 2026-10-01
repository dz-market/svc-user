package config

import pconfig "github.com/dz-market/platform/config"

func Load() (Config, error) {
	cfg, err := pconfig.Load[Config]()
	if err != nil {
		return Config{}, err
	}

	if cfg.Kafka.Consumer.ClientID == "" {
		cfg.Kafka.Consumer.ClientID = cfg.ServiceName
	}

	if cfg.Kafka.Consumer.GroupID == "" {
		cfg.Kafka.Consumer.GroupID = cfg.ServiceName
	}

	return cfg, nil
}
