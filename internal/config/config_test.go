package config

import (
	"testing"
	"time"
)

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "default config should be valid",
			config:  DefaultConfig(),
			wantErr: false,
		},
		{
			name: "invalid http port",
			config: &Config{
				Server: ServerConfig{
					HTTPPort: 0,
					GRPCPort: 9090,
				},
				Storage:     DefaultConfig().Storage,
				Etcd:        DefaultConfig().Etcd,
				Replication: DefaultConfig().Replication,
				Logging:     DefaultConfig().Logging,
			},
			wantErr: true,
		},
		{
			name: "same http and grpc port",
			config: &Config{
				Server: ServerConfig{
					HTTPPort: 8080,
					GRPCPort: 8080,
				},
				Storage:     DefaultConfig().Storage,
				Etcd:        DefaultConfig().Etcd,
				Replication: DefaultConfig().Replication,
				Logging:     DefaultConfig().Logging,
			},
			wantErr: true,
		},
		{
			name: "invalid replication factor",
			config: &Config{
				Server:  DefaultConfig().Server,
				Storage: DefaultConfig().Storage,
				Etcd:    DefaultConfig().Etcd,
				Replication: ReplicationConfig{
					Factor:              0,
					Strategy:            "async",
					MinReplicasForWrite: 1,
				},
				Logging: DefaultConfig().Logging,
			},
			wantErr: true,
		},
		{
			name: "invalid replication strategy",
			config: &Config{
				Server:  DefaultConfig().Server,
				Storage: DefaultConfig().Storage,
				Etcd:    DefaultConfig().Etcd,
				Replication: ReplicationConfig{
					Factor:              3,
					Strategy:            "invalid",
					MinReplicasForWrite: 1,
				},
				Logging: DefaultConfig().Logging,
			},
			wantErr: true,
		},
		{
			name: "invalid logging level",
			config: &Config{
				Server:      DefaultConfig().Server,
				Storage:     DefaultConfig().Storage,
				Etcd:        DefaultConfig().Etcd,
				Replication: DefaultConfig().Replication,
				Logging: LoggingConfig{
					Level:  "invalid",
					Format: "json",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Server.HTTPPort != 5555 {
		t.Errorf("expected HTTPPort 5555, got %d", cfg.Server.HTTPPort)
	}

	if cfg.Server.GRPCPort != 5556 {
		t.Errorf("expected GRPCPort 5556, got %d", cfg.Server.GRPCPort)
	}

	if cfg.Storage.MemoryStore.MaxAge != 2*time.Hour {
		t.Errorf("expected MaxAge 2h, got %v", cfg.Storage.MemoryStore.MaxAge)
	}

	if cfg.Replication.Factor != 3 {
		t.Errorf("expected replication factor 3, got %d", cfg.Replication.Factor)
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("default config should be valid: %v", err)
	}
}

func TestConfigHelpers(t *testing.T) {
	cfg := DefaultConfig()

	if !cfg.IsProduction() {
		t.Error("default config should be production mode")
	}

	cfg.Logging.Level = "debug"
	cfg.Logging.Format = "console"

	if !cfg.IsDevelopment() {
		t.Error("config with debug/console should be development mode")
	}

	dataPath := cfg.GetDataPath("test.db")
	if dataPath != "data/test.db" {
		t.Errorf("expected 'data/test.db', got %s", dataPath)
	}
}
