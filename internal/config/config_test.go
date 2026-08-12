package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDBConfig_DSN(t *testing.T) {
	cfg := DBConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "secret",
		DBName:   "bridge",
		SSLMode:  "disable",
	}
	want := "host=localhost port=5432 user=postgres password=secret dbname=bridge sslmode=disable"
	if got := cfg.DSN(); got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
}

func TestConfig_FindDstChain(t *testing.T) {
	cfg := &Config{
		ChannelChains: []ChannelChain{
			{SrcChainID: "dev", DstChainID: "11155111", SrcChannelID: 2, DstChannelID: 28},
			{SrcChainID: "11155111", DstChainID: "dev", SrcChannelID: 28, DstChannelID: 2},
		},
	}

	cases := []struct {
		name         string
		srcChainID   string
		srcChannelID int
		want         string
	}{
		{"gno to evm", "dev", 2, "11155111"},
		{"evm to gno", "11155111", 28, "dev"},
		{"unknown chain", "unknown", 2, ""},
		{"unknown channel", "dev", 99, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := cfg.FindDstChain(tc.srcChainID, tc.srcChannelID); got != tc.want {
				t.Errorf("FindDstChain(%q, %d) = %q, want %q", tc.srcChainID, tc.srcChannelID, got, tc.want)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	content := `
[server]
port = 8080

[relayer_db]
host = "localhost"
port = 5432
user = "postgres"
password = "secret"
dbname = "relayer"
sslmode = "disable"

[app_db]
host = "localhost"
port = 5432
user = "postgres"
password = "secret"
dbname = "bridge"
sslmode = "disable"

[indexer]
poll_interval_sec = 5
batch_size = 100

[[channel_chains]]
src_chain_id = "dev"
dst_chain_id = "11155111"
src_channel_id = 2
dst_channel_id = 28
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Server.Port != 8080 {
		t.Errorf("Server.Port = %d, want 8080", cfg.Server.Port)
	}
	if cfg.RelayerDB.DBName != "relayer" {
		t.Errorf("RelayerDB.DBName = %q, want %q", cfg.RelayerDB.DBName, "relayer")
	}
	if cfg.BridgeDB.DBName != "bridge" {
		t.Errorf("BridgeDB.DBName = %q, want %q", cfg.BridgeDB.DBName, "bridge")
	}
	if cfg.Indexer.PollIntervalSec != 5 || cfg.Indexer.BatchSize != 100 {
		t.Errorf("Indexer = %+v, want PollIntervalSec=5 BatchSize=100", cfg.Indexer)
	}
	if len(cfg.ChannelChains) != 1 || cfg.ChannelChains[0].DstChainID != "11155111" {
		t.Errorf("ChannelChains = %+v, want one entry with dst_chain_id=11155111", cfg.ChannelChains)
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	if _, err := Load("/nonexistent/config.toml"); err == nil {
		t.Error("Load() with nonexistent path: want error, got nil")
	}
}

func TestLoad_MalformedTOML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte("this is not valid toml ["), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Error("Load() with malformed TOML: want error, got nil")
	}
}
