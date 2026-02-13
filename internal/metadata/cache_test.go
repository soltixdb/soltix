package metadata

import (
	"testing"
	"time"
)

func TestNewKVCache(t *testing.T) {
	ttl := 10 * time.Second
	cache := NewKVCache(ttl)

	if cache == nil {
		t.Fatal("Expected cache to be created, got nil")
		return
	}

	if cache.ttl != ttl {
		t.Errorf("Expected TTL %v, got %v", ttl, cache.ttl)
	}

	if cache.entries == nil {
		t.Error("Expected entries map to be initialized")
	}
}

func TestKVCache_SetAndGet(t *testing.T) {
	cache := NewKVCache(1 * time.Second)

	tests := []struct {
		name  string
		key   string
		value string
	}{
		{
			name:  "simple_key_value",
			key:   "test-key",
			value: "test-value",
		},
		{
			name:  "key_with_prefix",
			key:   "/soltix/databases/db1/metadata",
			value: `{"name":"db1"}`,
		},
		{
			name:  "empty_value",
			key:   "empty-key",
			value: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache.Set(tt.key, tt.value)

			value, ok := cache.Get(tt.key)
			if !ok {
				t.Error("Expected key to exist in cache")
			}

			if value != tt.value {
				t.Errorf("Expected value %q, got %q", tt.value, value)
			}
		})
	}
}

func TestKVCache_GetNonExistent(t *testing.T) {
	cache := NewKVCache(1 * time.Second)

	value, ok := cache.Get("nonexistent-key")
	if ok {
		t.Error("Expected key to not exist")
	}

	if value != "" {
		t.Errorf("Expected empty value, got %q", value)
	}
}

func TestKVCache_GetExpired(t *testing.T) {
	cache := NewKVCache(100 * time.Millisecond)

	cache.Set("test-key", "test-value")

	value, ok := cache.Get("test-key")
	if !ok {
		t.Fatal("Expected key to exist immediately after setting")
	}
	if value != "test-value" {
		t.Errorf("Expected value 'test-value', got %q", value)
	}

	time.Sleep(150 * time.Millisecond)

	value, ok = cache.Get("test-key")
	if ok {
		t.Error("Expected key to be expired")
	}
	if value != "" {
		t.Errorf("Expected empty value for expired key, got %q", value)
	}
}

func TestKVCache_Delete(t *testing.T) {
	cache := NewKVCache(1 * time.Second)

	cache.Set("test-key", "test-value")

	_, ok := cache.Get("test-key")
	if !ok {
		t.Fatal("Expected key to exist after setting")
	}

	cache.Delete("test-key")

	value, ok := cache.Get("test-key")
	if ok {
		t.Error("Expected key to be deleted")
	}
	if value != "" {
		t.Errorf("Expected empty value for deleted key, got %q", value)
	}
}

func TestKVCache_DeletePrefix(t *testing.T) {
	cache := NewKVCache(1 * time.Second)

	cache.Set("/soltix/db1/coll1", "value1")
	cache.Set("/soltix/db1/coll2", "value2")
	cache.Set("/soltix/db2/coll1", "value3")
	cache.Set("/other/key", "value4")

	cache.DeletePrefix("/soltix/db1")

	_, ok := cache.Get("/soltix/db1/coll1")
	if ok {
		t.Error("Expected /soltix/db1/coll1 to be deleted")
	}

	_, ok = cache.Get("/soltix/db1/coll2")
	if ok {
		t.Error("Expected /soltix/db1/coll2 to be deleted")
	}

	value, ok := cache.Get("/soltix/db2/coll1")
	if !ok {
		t.Error("Expected /soltix/db2/coll1 to still exist")
	}
	if value != "value3" {
		t.Errorf("Expected value 'value3', got %q", value)
	}

	value, ok = cache.Get("/other/key")
	if !ok {
		t.Error("Expected /other/key to still exist")
	}
	if value != "value4" {
		t.Errorf("Expected value 'value4', got %q", value)
	}
}

func TestKVCache_Clear(t *testing.T) {
	cache := NewKVCache(1 * time.Second)

	cache.Set("key1", "value1")
	cache.Set("key2", "value2")
	cache.Set("key3", "value3")

	if _, ok := cache.Get("key1"); !ok {
		t.Fatal("Expected key1 to exist")
	}
	if _, ok := cache.Get("key2"); !ok {
		t.Fatal("Expected key2 to exist")
	}

	cache.Clear()

	if _, ok := cache.Get("key1"); ok {
		t.Error("Expected key1 to be cleared")
	}
	if _, ok := cache.Get("key2"); ok {
		t.Error("Expected key2 to be cleared")
	}
	if _, ok := cache.Get("key3"); ok {
		t.Error("Expected key3 to be cleared")
	}
}

func TestKVCache_Stats(t *testing.T) {
	cache := NewKVCache(100 * time.Millisecond)

	cache.Set("key1", "value1")
	cache.Set("key2", "value2")
	cache.Set("key3", "value3")

	stats := cache.Stats()
	if stats == nil {
		t.Fatal("Expected stats to be returned")
	}

	totalEntries, ok := stats["total_entries"].(int)
	if !ok {
		t.Fatal("Expected total_entries in stats")
	}
	if totalEntries != 3 {
		t.Errorf("Expected 3 total entries, got %d", totalEntries)
	}

	activeEntries, ok := stats["active_entries"].(int)
	if !ok {
		t.Fatal("Expected active_entries in stats")
	}
	if activeEntries != 3 {
		t.Errorf("Expected 3 active entries, got %d", activeEntries)
	}

	time.Sleep(150 * time.Millisecond)

	stats = cache.Stats()
	totalEntries = stats["total_entries"].(int)
	activeEntries = stats["active_entries"].(int)

	if totalEntries != 3 {
		t.Errorf("Expected 3 total entries (before cleanup), got %d", totalEntries)
	}
	if activeEntries != 0 {
		t.Errorf("Expected 0 active entries after expiration, got %d", activeEntries)
	}
}

func TestKVCache_ConcurrentAccess(t *testing.T) {
	cache := NewKVCache(1 * time.Second)

	done := make(chan bool)

	go func() {
		for i := 0; i < 100; i++ {
			cache.Set("key", "value")
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			cache.Get("key")
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			cache.Delete("key")
		}
		done <- true
	}()

	<-done
	<-done
	<-done
}
