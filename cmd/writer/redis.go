package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisClient struct {
	clusterClient *redis.ClusterClient
}

func NewRedisClient(addrs []string, writeTimeout, readTimeout, dialTimeout time.Duration) *RedisClient {
	return &RedisClient{
		clusterClient: redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        addrs,
			WriteTimeout: writeTimeout,
			ReadTimeout:  readTimeout,
			DialTimeout:  dialTimeout,
			// Connection pool optimizations to match rueidis performance characteristics
			PoolSize:     10,                         // Match rueidis PipelineMultiplex (2^5 = 32 connections per node, but go-redis uses per-node pools)
			MinIdleConns: 5,                          // Keep minimum idle connections to reduce connection overhead
			MaxRetries:   0,                          // Disable retries to match rueidis DisableRetry: true
			PoolTimeout:  writeTimeout + readTimeout, // Timeout for getting connection from pool
			IdleTimeout:  5 * time.Minute,            // Close idle connections after 5 minutes
		}),
	}
}

func (r *RedisClient) Name() string {
	return "redis"
}

func (r *RedisClient) HSetExpire(ctx context.Context, key string, cols []string, data map[string]string, ttl time.Duration) error {
	// Convert cols and data to the format expected by HSETEXPIRE
	// HSETEXPIRE key ttl field1 value1 field2 value2 ...
	args := make([]interface{}, 0, len(cols)*2+2)
	args = append(args, "HSETEXPIRE", key, strconv.Itoa(int(ttl.Seconds())))

	for _, col := range cols {
		val, ok := data[col]
		if !ok {
			return fmt.Errorf("field %s not found in data", col)
		}
		args = append(args, col, val)
	}

	// Use Do to execute HSETEXPIRE directly as a single atomic command
	// Not compatible with redis, only with kvrocks?
	err := r.clusterClient.Do(ctx, args...).Err()
	return err
}

func (r *RedisClient) Close() {
	r.clusterClient.Close()
}
