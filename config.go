package main

import (
	"fmt"
	"strings"
	"time"

	collector "github.com/luanruisong/hyperliquid-collector"
	"github.com/spf13/viper"
)

func loadConfig() (string, collector.Config, error) {
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath("./config")
	v.SetDefault("server.address", ":8080")
	v.SetDefault("collector.max_record_bytes", 0)
	v.SetDefault("collector.retention", "0s")
	if err := v.ReadInConfig(); err != nil {
		return "", collector.Config{}, fmt.Errorf("read config: %w", err)
	}

	var cfg struct {
		Server struct {
			Address string
		}
		Collector struct {
			DataDir        string `mapstructure:"data_dir"`
			CheckpointPath string `mapstructure:"checkpoint_path"`
			MaxRecordBytes int    `mapstructure:"max_record_bytes"`
			Retention      string `mapstructure:"retention"`
		}
	}
	if err := v.UnmarshalExact(&cfg); err != nil {
		return "", collector.Config{}, fmt.Errorf("decode config: %w", err)
	}
	if strings.TrimSpace(cfg.Server.Address) == "" {
		return "", collector.Config{}, fmt.Errorf("server.address is required")
	}
	retention, err := time.ParseDuration(cfg.Collector.Retention)
	if err != nil {
		return "", collector.Config{}, fmt.Errorf("collector.retention: %w", err)
	}
	if retention < 0 {
		return "", collector.Config{}, fmt.Errorf("collector.retention must be non-negative")
	}
	return cfg.Server.Address, collector.Config{
		DataDir:        cfg.Collector.DataDir,
		CheckpointPath: cfg.Collector.CheckpointPath,
		MaxRecordBytes: cfg.Collector.MaxRecordBytes,
		Retention:      retention,
	}, nil
}
