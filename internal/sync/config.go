package sync

import "time"

// Config holds configuration for the sync package
type Config struct {
	// Enabled enables/disables sync functionality
	Enabled bool `mapstructure:"enabled"`

	// StartupSync enables sync on node startup
	StartupSync bool `mapstructure:"startup_sync"`

	// StartupTimeout is the maximum time to wait for startup sync
	StartupTimeout time.Duration `mapstructure:"startup_timeout"`

	// SyncBatchSize is the number of data points per batch during sync
	SyncBatchSize int `mapstructure:"sync_batch_size"`

	// MaxConcurrentSyncs is the maximum number of concurrent shard syncs
	MaxConcurrentSyncs int `mapstructure:"max_concurrent_syncs"`

	// SyncTimeout is the timeout for syncing a single shard
	SyncTimeout time.Duration `mapstructure:"sync_timeout"`

	// RetryAttempts is the number of retry attempts for failed sync
	RetryAttempts int `mapstructure:"retry_attempts"`

	// RetryDelay is the delay between retry attempts
	RetryDelay time.Duration `mapstructure:"retry_delay"`

	// AntiEntropy configuration
	AntiEntropy AntiEntropyConfig `mapstructure:"anti_entropy"`
}

// AntiEntropyConfig holds configuration for background anti-entropy repair
type AntiEntropyConfig struct {
	// Enabled enables background anti-entropy
	Enabled bool `mapstructure:"enabled"`

	// Interval is how often to run anti-entropy checks
	Interval time.Duration `mapstructure:"interval"`

	// BatchSize is the number of data points to compare per batch
	BatchSize int `mapstructure:"batch_size"`

	// ChecksumWindow is the time window for checksum comparison
	ChecksumWindow time.Duration `mapstructure:"checksum_window"`
}

// DefaultConfig returns the default sync configuration
func DefaultConfig() Config {
	return Config{
		Enabled:            true,
		StartupSync:        true,
		StartupTimeout:     5 * time.Minute,
		SyncBatchSize:      1000,
		MaxConcurrentSyncs: 5,
		SyncTimeout:        10 * time.Minute,
		RetryAttempts:      3,
		RetryDelay:         5 * time.Second,
		AntiEntropy: AntiEntropyConfig{
			Enabled:        true,
			Interval:       1 * time.Hour,
			BatchSize:      10000,
			ChecksumWindow: 24 * time.Hour,
		},
	}
}

// Validate validates the sync configuration
func (c *Config) Validate() error {
	if c.SyncBatchSize <= 0 {
		c.SyncBatchSize = 1000
	}
	if c.MaxConcurrentSyncs <= 0 {
		c.MaxConcurrentSyncs = 5
	}
	if c.StartupTimeout <= 0 {
		c.StartupTimeout = 5 * time.Minute
	}
	if c.SyncTimeout <= 0 {
		c.SyncTimeout = 10 * time.Minute
	}
	if c.RetryAttempts <= 0 {
		c.RetryAttempts = 3
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = 5 * time.Second
	}
	if c.AntiEntropy.Interval <= 0 {
		c.AntiEntropy.Interval = 1 * time.Hour
	}
	if c.AntiEntropy.BatchSize <= 0 {
		c.AntiEntropy.BatchSize = 10000
	}
	if c.AntiEntropy.ChecksumWindow <= 0 {
		c.AntiEntropy.ChecksumWindow = 24 * time.Hour
	}
	return nil
}
