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

func (r *RueidisClient) Name() string {
	return "rueidis"
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

