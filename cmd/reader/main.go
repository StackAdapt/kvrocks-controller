package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/redis/rueidis"
	"go.uber.org/zap"
)

func main() {
	// goal is to spam reading and client connections
	for i := 0; i < 5; i++ {
		go func() {
			client, err := rueidis.NewClient(
				rueidis.ClientOption{
					InitAddress:       []string{"127.0.0.1:7770"},
					ShuffleInit:       true,
					ConnWriteTimeout:  time.Millisecond * 300,
					DisableCache:      true, // client cache is not enabled on kvrocks
					PipelineMultiplex: 5,
					MaxFlushDelay:     50 * time.Microsecond,
					AlwaysPipelining:  true,
					DisableTCPNoDelay: true,
					DisableRetry:      true,
				},
			)
			if err != nil {
				logger.Get().Error("unable to get rueidis client", zap.Error(err))
				return
			}
			ctx := context.Background()
			for i := 0; i < 1000000; i++ {
				_, err := hGetAll(ctx, time.Second, client, fmt.Sprintf("hello:%d", i))
				if err != nil {
					logger.Get().Error("err calling hGetAll", zap.Error(err))
				}
			}
			logger.Get().Info("done")
		}()
	}

	fmt.Println("Press Enter to exit...")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
	fmt.Println("Goodbye!")
}

// hGetAll queries from a hashmap and returns all fields and values of the hashmap
// it returns a map of field names to their values as strings
// returns an empty map if the key does not exist
func hGetAll(
	ctx context.Context,
	timeout time.Duration,
	client rueidis.Client,
	key string,
) (map[string]string, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := client.B().Hgetall().Key(key).Build()
	resp := client.Do(timeoutCtx, cmd)
	data, err := resp.AsStrMap()
	if err != nil {
		return nil, err
	}

	return data, nil
}
