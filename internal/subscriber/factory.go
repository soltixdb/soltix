package subscriber

import (
	"fmt"
	"strings"

	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/utils"
)

// NewSubscriber creates a new Subscriber based on the queue configuration
func NewSubscriber(cfg config.QueueConfig, subCfg Config) (Subscriber, error) {
	queueType := utils.QueueType(strings.ToLower(cfg.Type))

	// Default to NATS if not specified
	if queueType == "" {
		queueType = utils.QueueTypeNATS
	}

	switch queueType {
	case utils.QueueTypeNATS:
		return NewNATSSubscriber(cfg.URL, subCfg.NodeID, subCfg.ConsumerGroup)
	case utils.QueueTypeRedis:
		// Parse Redis URL to get address, or use URL directly as address
		addr := cfg.URL
		if addr == "" {
			addr = "localhost:6379"
		}
		// Get stream prefix from config, default to "soltix"
		streamPrefix := cfg.RedisStream
		if streamPrefix == "" {
			streamPrefix = "soltix"
		}
		return NewRedisSubscriber(addr, cfg.Password, cfg.RedisDB, streamPrefix, subCfg.ConsumerGroup, subCfg.NodeID)
	case utils.QueueTypeKafka:
		return NewKafkaSubscriber(cfg.KafkaBrokers, subCfg.ConsumerGroup)
	case utils.QueueTypeMemory:
		return NewMemorySubscriber()
	default:
		return nil, fmt.Errorf("unsupported queue type: %s", queueType)
	}
}
