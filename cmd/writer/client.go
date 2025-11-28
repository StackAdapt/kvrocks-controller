package main

import (
	"context"
	"time"
)

// Client defines the interface for Redis hash operations
type Client interface {
	// HSetExpire atomically sets hash fields and sets a TTL on the key.
	// For KVRocks, this uses the HSETEXPIRE command.
	// For standard Redis, this uses HSet followed by Expire.
	HSetExpire(ctx context.Context, key string, cols []string, data map[string]string, ttl time.Duration) error

	// Close closes the client connection
	Close()

	Name() string
}

