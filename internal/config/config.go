package config

import (
	"fmt"
	"time"
)

// Config represents the complete application configuration
type Config struct {
	Server      ServerConfig      `mapstructure:"server"`
	Storage     StorageConfig     `mapstructure:"storage"`
	Etcd        EtcdConfig        `mapstructure:"etcd"`
	Queue       QueueConfig       `mapstructure:"queue"`
	Replication ReplicationConfig `mapstructure:"replication"`
	Coordinator CoordinatorConfig `mapstructure:"coordinator"`
	Auth        AuthConfig        `mapstructure:"auth"`
	Logging     LoggingConfig     `mapstructure:"logging"`
}

// CoordinatorConfig for sharding and hashing
type CoordinatorConfig struct {
	HashThreshold  int                `mapstructure:"hash_threshold"`
	VNodeCount     int                `mapstructure:"vnode_count"`
	ReplicaFactor  int                `mapstructure:"replica_factor"`
	ShardKeyFormat string             `mapstructure:"shard_key_format"`
	TotalGroups    int                `mapstructure:"total_groups"`  // Number of logical groups for device-based sharding (default: 256)
	AutoAssigner   AutoAssignerConfig `mapstructure:"auto_assigner"` // Automatic group assignment config
}

// AutoAssignerConfig for automatic group-to-node assignment
type AutoAssignerConfig struct {
	Enabled            bool          `mapstructure:"enabled"`             // Enable automatic group assignment (default: true)
	PollInterval       time.Duration `mapstructure:"poll_interval"`       // How often to check for node changes (default: 15s)
	RebalanceOnJoin    bool          `mapstructure:"rebalance_on_join"`   // Rebalance groups when new node joins (default: true)
	RebalanceThreshold int           `mapstructure:"rebalance_threshold"` // Max group count diff before rebalance (default: 10)
}

// AuthConfig represents authentication configuration
type AuthConfig struct {
	Enabled bool     `mapstructure:"enabled"`  // Enable/disable API key authentication
	APIKeys []string `mapstructure:"api_keys"` // List of valid API keys
}

// ServerConfig represents server configuration
type ServerConfig struct {
	Host     string `mapstructure:"host"`      // Bind address for server (e.g., 0.0.0.0 for all interfaces)
	HTTPPort int    `mapstructure:"http_port"` // HTTP server port
	GRPCPort int    `mapstructure:"grpc_port"` // gRPC server port
	// GRPCHost is the advertise address for gRPC service discovery
	// Used as fallback when Host is 0.0.0.0 and auto IP detection fails
	// Useful for:
	// - Containers/VMs with complex networking where IP detection doesn't work
	// - Specific network interface selection (e.g., prefer eth1 over eth0)
	// - Testing environments with custom DNS/hostnames
	// Examples: "localhost", "192.168.1.100", "storage-node-01.internal"
	GRPCHost string `mapstructure:"grpc_host"`
}

// StorageConfig represents storage configuration
type StorageConfig struct {
	NodeID      string            `mapstructure:"node_id"`
	DataDir     string            `mapstructure:"data_dir"`
	Timezone    string            `mapstructure:"timezone"` // Timezone for data storage and sharding (e.g., "Asia/Tokyo", "+09:00", "UTC")
	MemoryStore MemoryStoreConfig `mapstructure:"memory_store"`
	Columnar    ColumnarConfig    `mapstructure:"multipart"` // Multi-part storage settings
	Sync        SyncConfig        `mapstructure:"sync"`      // Sync settings for replica synchronization
}

// ColumnarConfig represents columnar columnar storage configuration
type ColumnarConfig struct {
	MaxRowsPerPart     int   `mapstructure:"max_rows_per_part"`     // Primary split criterion (default: 10000)
	MaxPartSize        int64 `mapstructure:"max_part_size"`         // Safety limit in bytes (default: 64MB)
	MinRowsPerPart     int   `mapstructure:"min_rows_per_part"`     // Don't split if less than this (default: 1000)
	MaxDevicesPerGroup int   `mapstructure:"max_devices_per_group"` // Max devices per device group (default: 50)
}

// SyncConfig represents sync configuration for replica synchronization
type SyncConfig struct {
	Enabled            bool              `mapstructure:"enabled"`              // Enable sync functionality
	StartupSync        bool              `mapstructure:"startup_sync"`         // Sync on startup
	StartupTimeout     time.Duration     `mapstructure:"startup_timeout"`      // Timeout for startup sync
	SyncBatchSize      int               `mapstructure:"sync_batch_size"`      // Batch size for writing points
	MaxConcurrentSyncs int               `mapstructure:"max_concurrent_syncs"` // Max concurrent shard syncs
	AntiEntropy        AntiEntropyConfig `mapstructure:"anti_entropy"`         // Anti-entropy settings
}

// AntiEntropyConfig represents anti-entropy background sync configuration
type AntiEntropyConfig struct {
	Enabled   bool          `mapstructure:"enabled"`    // Enable anti-entropy
	Interval  time.Duration `mapstructure:"interval"`   // Check interval
	BatchSize int           `mapstructure:"batch_size"` // Batch size for checksum calculation
}

// MemoryStoreConfig represents memory store configuration
type MemoryStoreConfig struct {
	MaxAge  time.Duration `mapstructure:"max_age"`
	MaxSize int           `mapstructure:"max_size"`
}

// EtcdConfig represents etcd configuration
type EtcdConfig struct {
	Endpoints   []string      `mapstructure:"endpoints"`
	DialTimeout time.Duration `mapstructure:"dial_timeout"`
	Username    string        `mapstructure:"username"`
	Password    string        `mapstructure:"password"`
}

// QueueConfig represents message queue configuration
type QueueConfig struct {
	Type     string `mapstructure:"type"`     // Queue type: nats (default), redis, kafka, memory
	URL      string `mapstructure:"url"`      // Queue server URL (e.g., nats://localhost:4222, redis://localhost:6379)
	Username string `mapstructure:"username"` // Optional authentication
	Password string `mapstructure:"password"` // Optional authentication

	// Redis-specific options
	RedisDB       int    `mapstructure:"redis_db"`       // Redis database number (default: 0)
	RedisStream   string `mapstructure:"redis_stream"`   // Redis stream prefix (default: "soltix")
	RedisGroup    string `mapstructure:"redis_group"`    // Redis consumer group (default: "soltix-group")
	RedisConsumer string `mapstructure:"redis_consumer"` // Redis consumer name (default: hostname)

	// Kafka-specific options
	KafkaBrokers []string `mapstructure:"kafka_brokers"`  // Kafka broker addresses
	KafkaGroupID string   `mapstructure:"kafka_group_id"` // Kafka consumer group ID
}

// ReplicationConfig represents replication configuration
type ReplicationConfig struct {
	Factor              int    `mapstructure:"factor"`
	Strategy            string `mapstructure:"strategy"` // sync, async
	MinReplicasForWrite int    `mapstructure:"min_replicas_for_write"`
}

// LoggingConfig represents logging configuration
type LoggingConfig struct {
	Level      string `mapstructure:"level"`       // debug, info, warn, error
	Format     string `mapstructure:"format"`      // json, console
	OutputPath string `mapstructure:"output_path"` // stdout, stderr, file path
	TimeFormat string `mapstructure:"time_format"` // RFC3339, Unix, UnixMs, etc
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if err := c.Server.Validate(); err != nil {
		return fmt.Errorf("server config: %w", err)
	}

	if err := c.Storage.Validate(); err != nil {
		return fmt.Errorf("storage config: %w", err)
	}

	if err := c.Etcd.Validate(); err != nil {
		return fmt.Errorf("etcd config: %w", err)
	}

	if err := c.Replication.Validate(); err != nil {
		return fmt.Errorf("replication config: %w", err)
	}

	if err := c.Logging.Validate(); err != nil {
		return fmt.Errorf("logging config: %w", err)
	}

	return nil
}

// Validate validates server configuration
func (c *ServerConfig) Validate() error {
	if c.HTTPPort < 1 || c.HTTPPort > 65535 {
		return fmt.Errorf("invalid http_port: %d", c.HTTPPort)
	}

	if c.GRPCPort < 1 || c.GRPCPort > 65535 {
		return fmt.Errorf("invalid grpc_port: %d", c.GRPCPort)
	}

	if c.HTTPPort == c.GRPCPort {
		return fmt.Errorf("http_port and grpc_port cannot be the same")
	}

	return nil
}

// Validate validates storage configuration
func (c *StorageConfig) Validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("data_dir is required")
	}

	if c.MemoryStore.MaxAge <= 0 {
		return fmt.Errorf("memory_store.max_age must be positive")
	}

	if c.MemoryStore.MaxSize <= 0 {
		return fmt.Errorf("memory_store.max_size must be positive")
	}

	return nil
}

// Validate validates etcd configuration
func (c *EtcdConfig) Validate() error {
	if len(c.Endpoints) == 0 {
		return fmt.Errorf("etcd.endpoints is required")
	}

	if c.DialTimeout <= 0 {
		return fmt.Errorf("etcd.dial_timeout must be positive")
	}

	return nil
}

// Validate validates replication configuration
func (c *ReplicationConfig) Validate() error {
	if c.Factor < 1 {
		return fmt.Errorf("replication.factor must be at least 1")
	}

	if c.Factor > 10 {
		return fmt.Errorf("replication.factor cannot exceed 10")
	}

	if c.Strategy != "sync" && c.Strategy != "async" {
		return fmt.Errorf("replication.strategy must be 'sync' or 'async'")
	}

	if c.MinReplicasForWrite < 1 {
		return fmt.Errorf("replication.min_replicas_for_write must be at least 1")
	}

	if c.MinReplicasForWrite > c.Factor {
		return fmt.Errorf("replication.min_replicas_for_write cannot exceed replication.factor")
	}

	return nil
}

// Validate validates logging configuration
func (c *LoggingConfig) Validate() error {
	validLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}

	if !validLevels[c.Level] {
		return fmt.Errorf("logging.level must be one of: debug, info, warn, error")
	}

	validFormats := map[string]bool{
		"json":    true,
		"console": true,
	}

	if !validFormats[c.Format] {
		return fmt.Errorf("logging.format must be 'json' or 'console'")
	}

	return nil
}
