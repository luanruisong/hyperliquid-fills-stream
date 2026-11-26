package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	writeTestConfig(t, `server:
  address: "127.0.0.1:9000"
collector:
  data_dir: ./node-data
  checkpoint_path: ./checkpoint.json
  max_record_bytes: 1024
  retention: "72h"
`)

	address, cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if address != "127.0.0.1:9000" || cfg.DataDir != "./node-data" ||
		cfg.CheckpointPath != "./checkpoint.json" || cfg.MaxRecordBytes != 1024 ||
		cfg.Retention != 72*time.Hour {
		t.Fatalf("unexpected config: address=%q collector=%+v", address, cfg)
	}
}

func TestLoadConfigRetention(t *testing.T) {
	cases := []struct {
		name    string
		setting string
		invalid bool
	}{
		{"omitted", "", false},
		{"disabled", "  retention: \"0s\"\n", false},
		{"negative", "  retention: \"-1h\"\n", true},
		{"missing unit", "  retention: 72\n", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeTestConfig(t, "collector:\n  data_dir: ./node-data\n"+tc.setting)
			address, cfg, err := loadConfig()
			if tc.invalid {
				if err == nil || !strings.Contains(err.Error(), "collector.retention") {
					t.Fatalf("want collector.retention error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if address != ":8080" || cfg.Retention != 0 {
				t.Fatalf("unexpected defaults: address=%q retention=%v", address, cfg.Retention)
			}
		})
	}
}

func writeTestConfig(t *testing.T, yaml string) {
	t.Helper()
	t.Chdir(t.TempDir())
	if err := os.Mkdir("config", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("config", "config.yaml"), []byte(yaml), 0600); err != nil {
		t.Fatal(err)
	}
}
