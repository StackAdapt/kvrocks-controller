package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/StackAdapt/sa-go-adserver/logger"
	"github.com/redis/rueidis"
	"go.uber.org/zap"
)

func main() {
	logger.InitLogger(true)
	logger.InitNewLogger(true)
	// goal is to spam reading and client connections
	for i := 0; i < 5; i++ {
		go func() {
			client, err := rueidis.NewClient(
				rueidis.ClientOption{
					InitAddress:       []string{"127.0.0.1:7770"},
					ConnWriteTimeout:  10 * time.Second, // explicitly set to the rueidis default; otherwise, it would be computed from Dialer.KeepAlive - e.g 60s * 10
					ShuffleInit:       true,
					Dialer:            net.Dialer{KeepAlive: time.Second * 60}, // To decrease the pings
					DisableCache:      true,                                    // client cache is not enabled on kvrocks
					PipelineMultiplex: 4,
					MaxFlushDelay:     20 * time.Microsecond,
					AlwaysPipelining:  true,
					DisableRetry:      true,
					ClusterOption: rueidis.ClusterOption{
						AvoidRefreshOnRedirectMove: true,
					},
					QueueType: rueidis.QueueTypeFlowBuffer,
				},
			)
			if err != nil {
				logger.Error("unable to get rueidis client", zap.Error(err))
				return
			}
			ctx := context.Background()
			for i := 0; i < 10000000; i++ {
				key := i % 10
				time.Sleep(5000 * time.Millisecond)
				_, err := hGetAll(ctx, time.Second, client, fmt.Sprintf("hello:%d", key))
				if err != nil {
					logger.Error("err calling hGetAll", zap.Error(err))
				}
			}
			logger.Info("done")
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
