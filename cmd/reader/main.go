package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/StackAdapt/sa-go-adserver/logger"
	"github.com/VictoriaMetrics/metrics"
	"github.com/redis/rueidis"
	"go.uber.org/zap"
)

// intToAlphabetKey converts an integer to an alphabet-only string using bijective base-26 encoding
// 0 -> "a", 1 -> "b", ..., 25 -> "z", 26 -> "aa", 27 -> "ab", etc.
func intToAlphabetKey(n int64) string {
	if n < 0 {
		return ""
	}
	var result []byte
	for n >= 0 {
		result = append(result, byte('a'+n%26))
		n = n/26 - 1
		if n < 0 {
			break
		}
	}
	// Reverse the result
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return string(result)
}

func main() {
	logger.InitLogger(true)
	logger.InitNewLogger(true)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var clientsMu sync.Mutex
	var clients []Client

	numReaders := flag.Int("readers", runtime.GOMAXPROCS(0), "number of writer goroutines")
	readDelay := flag.Duration("delay", 0, "delay between writes (e.g., 1ms, 100us)")
	start := flag.Int("start", 0, "index to start at")
	clientType := flag.String("client", "rueidis", "client type: rueidis or redis")
	flag.Parse()
	logger.Info("starting service", zap.Int("readers", *numReaders), zap.Duration("delay", *readDelay), zap.Int("start", *start), zap.String("client", *clientType))
	// goal is to spam reading and client connections

	// Timeout configuration
	connWriteTimeout := 10 * time.Second
	kvRocksLiteReadTimeout := 1500 * time.Millisecond // context timeout

	metricsWg := sync.WaitGroup{}
	opts := &metrics.PushOptions{
		WaitGroup: &metricsWg,
	}
	err := metrics.InitPushWithOptions(ctx, "http://localhost:9201/metrics", 5*time.Second, true, opts)
	if err != nil {
		logger.Error("unable to initialize metrics with push", zap.Error(err))
		return
	}

	// Create and increment initialization counter with configuration metrics
	// Convert delay to milliseconds for readability
	delayMs := int64(*readDelay / time.Millisecond)
	connWriteTimeoutMs := int64(connWriteTimeout / time.Millisecond)
	hGetAllTimeoutMs := int64(kvRocksLiteReadTimeout / time.Millisecond)
	initCounter := metrics.GetOrCreateCounter(fmt.Sprintf(
		`kvrocks_reader_initialized_total{readers="%d",delay_ms="%d",start_index="%d",conn_write_timeout_ms="%d",hgetall_timeout_ms="%d",client_type="%s"}`,
		*numReaders, delayMs, *start, connWriteTimeoutMs, hGetAllTimeoutMs, *clientType,
	))
	initCounter.Inc()

	for i := 0; i < *numReaders; i++ {
		wg.Add(1)
		go func(id int, sleep time.Duration, startIndex int, connTimeout time.Duration, readTimeout time.Duration, clientTypeStr string) {
			defer wg.Done()
			var client Client

			switch clientTypeStr {
			case "redis":
				client = NewRedisClient(
					[]string{"kvrocks-byron-test.us-east-1.stackadapt:6379"},
					connTimeout,
					readTimeout,
					connTimeout,
				)
			case "rueidis":
				fallthrough
			default:
				rueidisClient, err := rueidis.NewClient(
					rueidis.ClientOption{
						InitAddress:       []string{"kvrocks-byron-test.us-east-1.stackadapt:6379"},
						ConnWriteTimeout:  connTimeout, // explicitly set to the rueidis default; otherwise, it would be computed from Dialer.KeepAlive - e.g 60s * 10
						ShuffleInit:       true,
						Dialer:            net.Dialer{KeepAlive: time.Second * 60}, // To decrease the pings
						DisableCache:      true,                                    // client cache is not enabled on kvrocks
						PipelineMultiplex: 5,
						MaxFlushDelay:     20 * time.Microsecond,
						AlwaysPipelining:  true,
						DisableRetry:      true,
						// ClusterOption: rueidis.ClusterOption{
						// 	AvoidRefreshOnRedirectMove: true,
						// },
						// QueueType: rueidis.QueueTypeFlowBuffer,
					},
				)
				if err != nil {
					logger.Error("unable to get rueidis client", zap.Error(err), zap.Int("id", id))
					return
				}
				client = NewRueidisClientFromClient(rueidisClient)
			}

			clientsMu.Lock()
			clients = append(clients, client)
			clientsMu.Unlock()

			for i := startIndex; ; i++ {
				// Check for context cancellation
				select {
				case <-ctx.Done():
					logger.Info("reader stopping due to context cancellation", zap.Int("id", id))
					return
				case <-time.After(sleep):
				}
				// Convert integer i to alphabet-only key before passing to hGetAll
				alphabetKey := intToAlphabetKey(int64(i))
				_, err := hGetAll(ctx, readTimeout, client, alphabetKey)
				if err != nil {
					// Check if error is due to context cancellation
					if ctx.Err() != nil {
						logger.Info("reader stopping due to context cancellation", zap.Int("id", id), zap.Error(err))
						return
					}
					logger.Error("err calling hGetAll", zap.Error(err), zap.Int("id", id))
				}
				if i%7000 == 0 {
					logger.Info("reading", zap.Int("keyIndex", i), zap.String("key", alphabetKey))
				}
			}
		}(i, *readDelay, *start, connWriteTimeout, kvRocksLiteReadTimeout, *clientType)
	}

	logger.Info("service running, waiting for shutdown signal")

	// Set up signal handling for systemd
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigChan
	logger.Info("received shutdown signal", zap.String("signal", sig.String()))
	logger.Info("shutting down gracefully...")

	// Cancel context to stop all readers
	cancel()

	// Wait for all readers to finish
	wg.Wait()
	metricsWg.Wait()

	// Close all clients
	logger.Info("closing clients")
	clientsMu.Lock()
	for _, client := range clients {
		if client != nil {
			client.Close()
		}
	}
	clientsMu.Unlock()

	logger.Info("shutdown complete")
}

// hGetAll queries from a hashmap and returns all fields and values of the hashmap
// it returns a map of field names to their values as strings
// returns an empty map if the key does not exist
func hGetAll(
	ctx context.Context,
	timeout time.Duration,
	client Client,
	key string,
) (map[string]string, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := time.Now()
	hGetAllSeconds := metrics.GetOrCreateHistogram(fmt.Sprintf(`kvrocks_command_seconds{command="hgetall",client="%s"}`, client.Name()))
	defer hGetAllSeconds.UpdateDuration(start)

	data, err := client.HGetAll(timeoutCtx, key)
	if err != nil {
		metrics.GetOrCreateCounter(fmt.Sprintf(`kvrocks_command_errors_total{command="hgetall",client="%s"}`, client.Name())).Inc()
		return nil, err
	}

	return data, nil
}
