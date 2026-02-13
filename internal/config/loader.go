package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Load loads configuration from file
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Set config file
	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		// Default config locations
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")           // Current directory
		v.AddConfigPath("./configs")   // Project configs directory
		v.AddConfigPath("./config")    // Alternative config directory
		v.AddConfigPath("/etc/soltix") // System-wide config
	}

	// Set defaults
	setDefaults(v)

	// Enable environment variable overrides
	v.SetEnvPrefix("SOLTIX")
	v.AutomaticEnv()

	// Read config file
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; use defaults
			return parseConfig(v)
		}
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	return parseConfig(v)
}

// setDefaults sets default configuration values
func setDefaults(v *viper.Viper) {
	// Server defaults
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.http_port", 5555)
	v.SetDefault("server.grpc_port", 5556)

	// Storage defaults
	v.SetDefault("storage.node_id", "storage-default-node")
	v.SetDefault("storage.data_dir", "./data")
	v.SetDefault("storage.wal_dir", "./wal")
	v.SetDefault("storage.memory_store.max_age", "2h")
	v.SetDefault("storage.memory_store.max_size", 100000)

	// Etcd defaults
	v.SetDefault("etcd.endpoints", []string{"http://localhost:2379"})
	v.SetDefault("etcd.dial_timeout", "5s")

	// Queue defaults
	v.SetDefault("queue.url", "nats://localhost:4222")

	// Replication defaults
	v.SetDefault("replication.factor", 3)
	v.SetDefault("replication.strategy", "async")
	v.SetDefault("replication.min_replicas_for_write", 1)

	// Logging defaults
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
	v.SetDefault("logging.output_path", "stdout")

	// Coordinator defaults
	v.SetDefault("coordinator.hash_threshold", 20)
	v.SetDefault("coordinator.vnode_count", 200)
	v.SetDefault("coordinator.replica_factor", 3)
	v.SetDefault("coordinator.shard_key_format", "daily")
}

// parseConfig parses viper config into Config struct
func parseConfig(v *viper.Viper) (*Config, error) {
	var cfg Config

	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

// LoadOrDefault loads configuration from file or returns default config
func LoadOrDefault(configPath string) *Config {
	cfg, err := Load(configPath)
	if err != nil {
		// Return default configuration
		return DefaultConfig()
	}
	return cfg
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Host:     "0.0.0.0",
			HTTPPort: 5555,
			GRPCPort: 5556,
		},
		Storage: StorageConfig{
			NodeID:  "storage-default-node",
			DataDir: "./data",
			MemoryStore: MemoryStoreConfig{
				MaxAge:  2 * time.Hour,
				MaxSize: 100000,
			},
			Sync: SyncConfig{
				Enabled:            true,
				StartupSync:        true,
				StartupTimeout:     5 * time.Minute,
				SyncBatchSize:      1000,
				MaxConcurrentSyncs: 5,
				AntiEntropy: AntiEntropyConfig{
					Enabled:   true,
					Interval:  1 * time.Hour,
					BatchSize: 10000,
				},
			},
		},
		Etcd: EtcdConfig{
			Endpoints:   []string{"http://localhost:2379"},
			DialTimeout: 5 * time.Second,
		},
		Replication: ReplicationConfig{
			Factor:              3,
			Strategy:            "async",
			MinReplicasForWrite: 1,
		},
		Logging: LoggingConfig{
			Level:      "info",
			Format:     "json",
			OutputPath: "stdout",
		},
	}
}
