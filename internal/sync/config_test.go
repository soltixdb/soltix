package sync

import (
	"testing"
	"time"
)

func TestDefaultConfig_AllFields(t *testing.T) {
	config := DefaultConfig()

	if !config.Enabled {
		t.Error("Expected Enabled=true")
	}
	if !config.StartupSync {
		t.Error("Expected StartupSync=true")
	}
	if config.StartupTimeout != 5*time.Minute {
		t.Errorf("Expected StartupTimeout=5m, got %v", config.StartupTimeout)
	}
	if config.SyncBatchSize != 1000 {
		t.Errorf("Expected SyncBatchSize=1000, got %d", config.SyncBatchSize)
	}
	if config.MaxConcurrentSyncs != 5 {
		t.Errorf("Expected MaxConcurrentSyncs=5, got %d", config.MaxConcurrentSyncs)
	}
	if config.SyncTimeout != 10*time.Minute {
		t.Errorf("Expected SyncTimeout=10m, got %v", config.SyncTimeout)
	}
	if config.RetryAttempts != 3 {
		t.Errorf("Expected RetryAttempts=3, got %d", config.RetryAttempts)
	}
	if config.RetryDelay != 5*time.Second {
		t.Errorf("Expected RetryDelay=5s, got %v", config.RetryDelay)
	}
	if !config.AntiEntropy.Enabled {
		t.Error("Expected AntiEntropy.Enabled=true")
	}
	if config.AntiEntropy.Interval != 1*time.Hour {
		t.Errorf("Expected AntiEntropy.Interval=1h, got %v", config.AntiEntropy.Interval)
	}
	if config.AntiEntropy.BatchSize != 10000 {
		t.Errorf("Expected AntiEntropy.BatchSize=10000, got %d", config.AntiEntropy.BatchSize)
	}
	if config.AntiEntropy.ChecksumWindow != 24*time.Hour {
		t.Errorf("Expected AntiEntropy.ChecksumWindow=24h, got %v", config.AntiEntropy.ChecksumWindow)
	}
}

func TestConfig_Validate_AllZeroValues(t *testing.T) {
	config := Config{}
	err := config.Validate()
	if err != nil {
		t.Fatalf("Validate should not return error, got: %v", err)
	}

	// All zero values should be corrected to defaults
	if config.SyncBatchSize != 1000 {
		t.Errorf("SyncBatchSize should default to 1000, got %d", config.SyncBatchSize)
	}
	if config.MaxConcurrentSyncs != 5 {
		t.Errorf("MaxConcurrentSyncs should default to 5, got %d", config.MaxConcurrentSyncs)
	}
	if config.StartupTimeout != 5*time.Minute {
		t.Errorf("StartupTimeout should default to 5m, got %v", config.StartupTimeout)
	}
	if config.SyncTimeout != 10*time.Minute {
		t.Errorf("SyncTimeout should default to 10m, got %v", config.SyncTimeout)
	}
	if config.RetryAttempts != 3 {
		t.Errorf("RetryAttempts should default to 3, got %d", config.RetryAttempts)
	}
	if config.RetryDelay != 5*time.Second {
		t.Errorf("RetryDelay should default to 5s, got %v", config.RetryDelay)
	}
	if config.AntiEntropy.Interval != 1*time.Hour {
		t.Errorf("AntiEntropy.Interval should default to 1h, got %v", config.AntiEntropy.Interval)
	}
	if config.AntiEntropy.BatchSize != 10000 {
		t.Errorf("AntiEntropy.BatchSize should default to 10000, got %d", config.AntiEntropy.BatchSize)
	}
	if config.AntiEntropy.ChecksumWindow != 24*time.Hour {
		t.Errorf("AntiEntropy.ChecksumWindow should default to 24h, got %v", config.AntiEntropy.ChecksumWindow)
	}
}

func TestConfig_Validate_NegativeValues(t *testing.T) {
	config := Config{
		SyncBatchSize:      -10,
		MaxConcurrentSyncs: -5,
		StartupTimeout:     -1 * time.Minute,
		SyncTimeout:        -10 * time.Minute,
		RetryAttempts:      -3,
		RetryDelay:         -5 * time.Second,
		AntiEntropy: AntiEntropyConfig{
			Interval:       -1 * time.Hour,
			BatchSize:      -100,
			ChecksumWindow: -24 * time.Hour,
		},
	}

	err := config.Validate()
	if err != nil {
		t.Fatalf("Validate should not return error, got: %v", err)
	}

	if config.SyncBatchSize != 1000 {
		t.Errorf("Negative SyncBatchSize should default to 1000, got %d", config.SyncBatchSize)
	}
	if config.MaxConcurrentSyncs != 5 {
		t.Errorf("Negative MaxConcurrentSyncs should default to 5, got %d", config.MaxConcurrentSyncs)
	}
	if config.StartupTimeout != 5*time.Minute {
		t.Errorf("Negative StartupTimeout should default to 5m, got %v", config.StartupTimeout)
	}
	if config.SyncTimeout != 10*time.Minute {
		t.Errorf("Negative SyncTimeout should default to 10m, got %v", config.SyncTimeout)
	}
	if config.RetryAttempts != 3 {
		t.Errorf("Negative RetryAttempts should default to 3, got %d", config.RetryAttempts)
	}
	if config.RetryDelay != 5*time.Second {
		t.Errorf("Negative RetryDelay should default to 5s, got %v", config.RetryDelay)
	}
	if config.AntiEntropy.Interval != 1*time.Hour {
		t.Errorf("Negative AE Interval should default to 1h, got %v", config.AntiEntropy.Interval)
	}
	if config.AntiEntropy.BatchSize != 10000 {
		t.Errorf("Negative AE BatchSize should default to 10000, got %d", config.AntiEntropy.BatchSize)
	}
	if config.AntiEntropy.ChecksumWindow != 24*time.Hour {
		t.Errorf("Negative AE ChecksumWindow should default to 24h, got %v", config.AntiEntropy.ChecksumWindow)
	}
}

func TestConfig_Validate_ValidValues_Unchanged(t *testing.T) {
	config := Config{
		Enabled:            true,
		StartupSync:        true,
		SyncBatchSize:      500,
		MaxConcurrentSyncs: 10,
		StartupTimeout:     3 * time.Minute,
		SyncTimeout:        15 * time.Minute,
		RetryAttempts:      5,
		RetryDelay:         10 * time.Second,
		AntiEntropy: AntiEntropyConfig{
			Enabled:        true,
			Interval:       30 * time.Minute,
			BatchSize:      5000,
			ChecksumWindow: 12 * time.Hour,
		},
	}

	err := config.Validate()
	if err != nil {
		t.Fatalf("Validate should not return error, got: %v", err)
	}

	// Valid values should not be changed
	if config.SyncBatchSize != 500 {
		t.Errorf("Valid SyncBatchSize should remain 500, got %d", config.SyncBatchSize)
	}
	if config.MaxConcurrentSyncs != 10 {
		t.Errorf("Valid MaxConcurrentSyncs should remain 10, got %d", config.MaxConcurrentSyncs)
	}
	if config.StartupTimeout != 3*time.Minute {
		t.Errorf("Valid StartupTimeout should remain 3m, got %v", config.StartupTimeout)
	}
	if config.SyncTimeout != 15*time.Minute {
		t.Errorf("Valid SyncTimeout should remain 15m, got %v", config.SyncTimeout)
	}
	if config.RetryAttempts != 5 {
		t.Errorf("Valid RetryAttempts should remain 5, got %d", config.RetryAttempts)
	}
	if config.RetryDelay != 10*time.Second {
		t.Errorf("Valid RetryDelay should remain 10s, got %v", config.RetryDelay)
	}
	if config.AntiEntropy.Interval != 30*time.Minute {
		t.Errorf("Valid AE Interval should remain 30m, got %v", config.AntiEntropy.Interval)
	}
	if config.AntiEntropy.BatchSize != 5000 {
		t.Errorf("Valid AE BatchSize should remain 5000, got %d", config.AntiEntropy.BatchSize)
	}
	if config.AntiEntropy.ChecksumWindow != 12*time.Hour {
		t.Errorf("Valid AE ChecksumWindow should remain 12h, got %v", config.AntiEntropy.ChecksumWindow)
	}
}

func TestConfig_Validate_PartialZeroValues(t *testing.T) {
	// Only some fields are zero, others are valid
	config := Config{
		SyncBatchSize:      0, // should default
		MaxConcurrentSyncs: 10,
		StartupTimeout:     3 * time.Minute,
		SyncTimeout:        0, // should default
		RetryAttempts:      5,
		RetryDelay:         0, // should default
		AntiEntropy: AntiEntropyConfig{
			Interval:       30 * time.Minute,
			BatchSize:      0, // should default
			ChecksumWindow: 12 * time.Hour,
		},
	}

	err := config.Validate()
	if err != nil {
		t.Fatalf("Validate should not return error: %v", err)
	}

	// Zero fields should be defaulted
	if config.SyncBatchSize != 1000 {
		t.Errorf("Zero SyncBatchSize should default to 1000, got %d", config.SyncBatchSize)
	}
	if config.SyncTimeout != 10*time.Minute {
		t.Errorf("Zero SyncTimeout should default to 10m, got %v", config.SyncTimeout)
	}
	if config.RetryDelay != 5*time.Second {
		t.Errorf("Zero RetryDelay should default to 5s, got %v", config.RetryDelay)
	}
	if config.AntiEntropy.BatchSize != 10000 {
		t.Errorf("Zero AE BatchSize should default to 10000, got %d", config.AntiEntropy.BatchSize)
	}

	// Non-zero fields should remain
	if config.MaxConcurrentSyncs != 10 {
		t.Errorf("Non-zero MaxConcurrentSyncs should remain 10, got %d", config.MaxConcurrentSyncs)
	}
	if config.StartupTimeout != 3*time.Minute {
		t.Errorf("Non-zero StartupTimeout should remain 3m, got %v", config.StartupTimeout)
	}
	if config.RetryAttempts != 5 {
		t.Errorf("Non-zero RetryAttempts should remain 5, got %d", config.RetryAttempts)
	}
}

func TestConfig_Validate_BoundaryValues(t *testing.T) {
	// Test with value = 1 (minimum valid)
	config := Config{
		SyncBatchSize:      1,
		MaxConcurrentSyncs: 1,
		StartupTimeout:     1 * time.Nanosecond,
		SyncTimeout:        1 * time.Nanosecond,
		RetryAttempts:      1,
		RetryDelay:         1 * time.Nanosecond,
		AntiEntropy: AntiEntropyConfig{
			Interval:       1 * time.Nanosecond,
			BatchSize:      1,
			ChecksumWindow: 1 * time.Nanosecond,
		},
	}

	err := config.Validate()
	if err != nil {
		t.Fatalf("Validate should not return error: %v", err)
	}

	// All values >= 1 should remain unchanged
	if config.SyncBatchSize != 1 {
		t.Errorf("SyncBatchSize=1 should remain, got %d", config.SyncBatchSize)
	}
	if config.MaxConcurrentSyncs != 1 {
		t.Errorf("MaxConcurrentSyncs=1 should remain, got %d", config.MaxConcurrentSyncs)
	}
	if config.RetryAttempts != 1 {
		t.Errorf("RetryAttempts=1 should remain, got %d", config.RetryAttempts)
	}
	if config.AntiEntropy.BatchSize != 1 {
		t.Errorf("AE BatchSize=1 should remain, got %d", config.AntiEntropy.BatchSize)
	}
}

func TestConfig_Validate_EnabledFlags_NotAffected(t *testing.T) {
	// Validate should not change Enabled/StartupSync flags
	config := Config{
		Enabled:     false,
		StartupSync: false,
		AntiEntropy: AntiEntropyConfig{
			Enabled: false,
		},
	}

	_ = config.Validate()

	if config.Enabled {
		t.Error("Validate should not change Enabled from false to true")
	}
	if config.StartupSync {
		t.Error("Validate should not change StartupSync from false to true")
	}
	if config.AntiEntropy.Enabled {
		t.Error("Validate should not change AntiEntropy.Enabled from false to true")
	}
}

func TestDefaultConfig_Validate_Idempotent(t *testing.T) {
	config := DefaultConfig()
	original := config // copy

	_ = config.Validate()

	// Validate on DefaultConfig should not change anything
	if config.SyncBatchSize != original.SyncBatchSize {
		t.Errorf("Validate changed SyncBatchSize from %d to %d", original.SyncBatchSize, config.SyncBatchSize)
	}
	if config.MaxConcurrentSyncs != original.MaxConcurrentSyncs {
		t.Errorf("Validate changed MaxConcurrentSyncs")
	}
	if config.StartupTimeout != original.StartupTimeout {
		t.Errorf("Validate changed StartupTimeout")
	}
	if config.SyncTimeout != original.SyncTimeout {
		t.Errorf("Validate changed SyncTimeout")
	}
	if config.RetryAttempts != original.RetryAttempts {
		t.Errorf("Validate changed RetryAttempts")
	}
	if config.RetryDelay != original.RetryDelay {
		t.Errorf("Validate changed RetryDelay")
	}
}
