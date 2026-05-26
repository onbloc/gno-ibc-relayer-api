package config

import (
	"fmt"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Server        ServerConfig   `toml:"server"`
	RelayerDB     DBConfig       `toml:"relayer_db"`
	AppDB         DBConfig       `toml:"app_db"`
	Indexer       IndexerConfig  `toml:"indexer"`
	ChannelChains []ChannelChain `toml:"channel_chains"`
}

type ChannelChain struct {
	SrcChainID   string `toml:"src_chain_id"`
	DstChainID   string `toml:"dst_chain_id"`
	SrcChannelID int    `toml:"src_channel_id"`
	DstChannelID int    `toml:"dst_channel_id"`
}

// FindDstChain returns the destination chain ID for a given source chain + channel.
// Returns "" if not found (e.g. union relay packets).
func (c *Config) FindDstChain(srcChainID string, srcChannelID int) string {
	for _, cc := range c.ChannelChains {
		if cc.SrcChainID == srcChainID && cc.SrcChannelID == srcChannelID {
			return cc.DstChainID
		}
	}
	return ""
}

type ServerConfig struct {
	Port int `toml:"port"`
}

type DBConfig struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	DBName   string `toml:"dbname"`
	SSLMode  string `toml:"sslmode"`
}

func (c DBConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.DBName, c.SSLMode,
	)
}

type IndexerConfig struct {
	PollIntervalSec int `toml:"poll_interval_sec"`
	BatchSize       int `toml:"batch_size"`
}

func Load(path string) (*Config, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, fmt.Errorf("config: decode %s: %w", path, err)
	}
	return &cfg, nil
}
