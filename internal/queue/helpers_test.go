package queue

import "github.com/nats-io/nats.go"

// Test-only helpers to keep existing test names while constructors are unexported.

func NewNATSQueue(url string) (*NATSQueue, error) {
	return newNATSQueue(url)
}

func NewNATSQueueWithConn(conn *nats.Conn) (*NATSQueue, error) {
	return newNATSQueueWithConn(conn)
}

func NewRedisQueue(cfg RedisConfig) (*RedisQueue, error) {
	return newRedisQueue(cfg)
}

func NewKafkaQueue(cfg KafkaConfig) (*KafkaQueue, error) {
	return newKafkaQueue(cfg)
}

func NewMemoryQueue() *MemoryQueue {
	return newMemoryQueue()
}
