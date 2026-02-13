package metadata

import (
	"sync"
	"time"
)

// CacheEntry represents a cached key-value entry
type CacheEntry struct {
	Value     string
	ExpiresAt time.Time
}

// KVCache provides in-memory caching for key-value operations
type KVCache struct {
	mu      sync.RWMutex
	entries map[string]*CacheEntry
	ttl     time.Duration
	stopCh  chan struct{}
}

// NewKVCache creates a new key-value cache
func NewKVCache(ttl time.Duration) *KVCache {
	cache := &KVCache{
		entries: make(map[string]*CacheEntry),
		ttl:     ttl,
		stopCh:  make(chan struct{}),
	}

	// Start cleanup goroutine
	go cache.cleanup()

	return cache
}

// Get retrieves a value from cache
func (c *KVCache) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[key]
	if !exists {
		return "", false
	}

	// Check expiration
	if time.Now().After(entry.ExpiresAt) {
		return "", false
	}

	return entry.Value, true
}

// Set stores a value in cache
func (c *KVCache) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[key] = &CacheEntry{
		Value:     value,
		ExpiresAt: time.Now().Add(c.ttl),
	}
}

// Delete removes a key from cache
func (c *KVCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.entries, key)
}

// DeletePrefix removes all keys with given prefix
func (c *KVCache) DeletePrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key := range c.entries {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			delete(c.entries, key)
		}
	}
}

// Clear removes all entries
func (c *KVCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]*CacheEntry)
}

// cleanup periodically removes expired entries
func (c *KVCache) cleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			for key, entry := range c.entries {
				if now.After(entry.ExpiresAt) {
					delete(c.entries, key)
				}
			}
			c.mu.Unlock()
		case <-c.stopCh:
			return
		}
	}
}

// Stop stops the cleanup goroutine
func (c *KVCache) Stop() {
	close(c.stopCh)
}

// Stats returns cache statistics
func (c *KVCache) Stats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	expired := 0
	now := time.Now()
	for _, entry := range c.entries {
		if now.After(entry.ExpiresAt) {
			expired++
		}
	}

	return map[string]interface{}{
		"total_entries":   len(c.entries),
		"expired_entries": expired,
		"active_entries":  len(c.entries) - expired,
		"ttl_seconds":     c.ttl.Seconds(),
	}
}
