package main

import (
	"context"
	"time"
)

// Client defines the interface for Redis hash operations
type Client interface {
	// Hmget retrieves the values of specified fields in a hash.
	// Returns []interface{} where each element is either a string or nil,
	// allowing distinction between empty strings and missing fields.
	Hmget(ctx context.Context, key string, fields ...string) ([]interface{}, error)

	// HGetAll retrieves all fields and values of a hash.
	// Returns a map of field names to their values as strings.
	// Returns an empty map if the key does not exist.
	HGetAll(ctx context.Context, key string) (map[string]string, error)

	// HSetExpire atomically sets hash fields and sets a TTL on the key.
	// For KVRocks, this uses the HSETEXPIRE command.
	// For standard Redis, this uses HSet followed by Expire.
	HSetExpire(ctx context.Context, key string, cols []string, data map[string]string, ttl time.Duration) error

	// Close closes the client connection
	Close()

	Name() string
}
