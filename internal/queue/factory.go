package queue

import (
	"fmt"
	"strings"

	"github.com/soltixdb/soltix/internal/config"
	"github.com/soltixdb/soltix/internal/utils"
)

// NewQueue creates a new Queue instance based on configuration
// Default is NATS if type is not specified
func NewQueue(cfg config.QueueConfig) (Queue, error) {
	queueType := utils.QueueType(strings.ToLower(cfg.Type))

	// Default to NATS if not specified
	if queueType == "" {
		queueType = utils.QueueTypeNATS
	}

	switch queueType {
	case utils.QueueTypeNATS:
		return newNATSQueue(cfg.URL)

	case utils.QueueTypeRedis:
		return newRedisQueue(RedisConfig{
			URL:      cfg.URL,
			Password: cfg.Password,
			DB:       cfg.RedisDB,
			Stream:   cfg.RedisStream,
			Group:    cfg.RedisGroup,
			Consumer: cfg.RedisConsumer,
		})

	case utils.QueueTypeKafka:
		return newKafkaQueue(KafkaConfig{
			Brokers: cfg.KafkaBrokers,
			GroupID: cfg.KafkaGroupID,
		})

	case utils.QueueTypeMemory:
		return newMemoryQueue(), nil

	default:
		return nil, fmt.Errorf("unsupported queue type: %s (supported: nats, redis, kafka, memory)", queueType)
	}
}

// NewPublisher creates a new Publisher instance based on configuration
// This is a convenience function when only publishing is needed
func NewPublisher(cfg config.QueueConfig) (Publisher, error) {
	return NewQueue(cfg)
}

// NewSubscriber creates a new Subscriber instance based on configuration
// This is a convenience function when only subscribing is needed
func NewSubscriber(cfg config.QueueConfig) (Subscriber, error) {
	return NewQueue(cfg)
}
