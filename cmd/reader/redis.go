package main

import (
	"context"
	"fmt"
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
		}),
	}
}

func (r *RedisClient) Name() string {
	return "redis"
}

func (r *RedisClient) Hmget(ctx context.Context, key string, fields ...string) ([]interface{}, error) {
	result, err := r.clusterClient.HMGet(ctx, key, fields...).Result()
	return result, err
}

func (r *RedisClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	result, err := r.clusterClient.HGetAll(ctx, key).Result()
	return result, err
}

func (r *RedisClient) HSetExpire(ctx context.Context, key string, cols []string, data map[string]string, ttl time.Duration) error {
	// Convert cols and data to the format expected by HSet
	fields := make([]interface{}, 0, len(cols)*2)
	for _, col := range cols {
		val, ok := data[col]
		if !ok {
			return fmt.Errorf("field %s not found in data", col)
		}
		fields = append(fields, col, val)
	}

	// Use pipeline to atomically execute HSet and Expire
	pipe := r.clusterClient.Pipeline()
	pipe.HSet(ctx, key, fields...)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisClient) Close() {
	r.clusterClient.Close()
}
