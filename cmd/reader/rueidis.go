package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/rueidis"
)

type RueidisClient struct {
	client rueidis.Client
}

func NewRueidisClient(options rueidis.ClientOption) (*RueidisClient, error) {
	client, err := rueidis.NewClient(options)
	if err != nil {
		return nil, err
	}
	return &RueidisClient{
		client: client,
	}, nil
}

func NewRueidisClientFromClient(client rueidis.Client) *RueidisClient {
	return &RueidisClient{
		client: client,
	}
}

func (r *RueidisClient) Hmget(ctx context.Context, key string, fields ...string) ([]interface{}, error) {
	cmd := r.client.B().Hmget().Key(key).Field(fields...).Build()
	resp := r.client.Do(ctx, cmd)
	msgs, err := resp.ToArray()
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(msgs))
	for i, msg := range msgs {
		if msg.IsNil() {
			result[i] = nil
		} else {
			s, err := msg.ToString()
			if err != nil {
				return nil, err
			}
			result[i] = s
		}
	}

	return result, nil
}

func (r *RueidisClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	cmd := r.client.B().Hgetall().Key(key).Build()
	resp := r.client.Do(ctx, cmd)
	data, err := resp.AsStrMap()
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (r *RueidisClient) HSetExpire(ctx context.Context, key string, cols []string, data map[string]string, ttl time.Duration) error {
	convertedSlice := make([]string, 0, len(cols)*2)
	for _, col := range cols {
		val, ok := data[col]
		if !ok {
			return fmt.Errorf("field %s not found in data", col)
		}

		convertedSlice = append(convertedSlice, col, val)
	}
	cmd := r.client.B().
		Arbitrary("HSETEXPIRE").
		Keys(key).
		Args(strconv.Itoa(int(ttl.Seconds()))).
		Args(convertedSlice...).
		Build()
	resp := r.client.Do(ctx, cmd)
	return resp.Error()
}

func (r *RueidisClient) Close() {
	r.client.Close()
}

