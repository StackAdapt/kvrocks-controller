package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/redis/rueidis"
	"go.uber.org/zap"
)

func main() {
	numOfWriters := runtime.GOMAXPROCS(0)
	writers := make([]*Writer, numOfWriters)
	for i := 0; i < numOfWriters; i++ {
		logger.Get().Info("creating writers", zap.Int("num", i))
		writer, err := NewWriter()
		if err != nil {
			logger.Get().Error("unable to get rueidis client", zap.Error(err))
			return
		}
		writers[i] = writer
	}

	ctx, cancel := context.WithCancel(context.Background())

	logger.Get().Info("creating payload")
	payload := []byte("123123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789456789")
	data := make(map[string][]byte)
	cols := []string{}
	for i := 0; i < 100; i++ {
		data[fmt.Sprintf("%d", i)] = payload
		cols = append(cols, fmt.Sprintf("%d", i))
	}

	var wg sync.WaitGroup

	logger.Get().Info("starting writers")
	for _, writer := range writers {
		wg.Add(1)
		go writer.Start(ctx, &wg, data, cols, 0*time.Millisecond)
		time.Sleep(0 * time.Millisecond)
	}

	logger.Get().Info("service running, waiting for shutdown signal")

	// Set up signal handling for systemd
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigChan
	logger.Get().Info("received shutdown signal", zap.String("signal", sig.String()))
	logger.Get().Info("shutting down gracefully...")

	cancel()
	wg.Wait()

	// Close all clients
	logger.Get().Info("closing clients")
	for _, writer := range writers {
		if writer != nil && writer.client != nil {
			writer.client.Close()
		}
	}

	logger.Get().Info("shutdown complete")
}

type Writer struct {
	client rueidis.Client
}

func NewWriter() (*Writer, error) {
	client, err := rueidis.NewClient(
		rueidis.ClientOption{
			InitAddress:       []string{"kvrocks-byron-test.us-east-1.stackadapt:6379"},
			ShuffleInit:       true,
			ConnWriteTimeout:  time.Millisecond * 100,
			DisableCache:      true, // client cache is not enabled on kvrocks
			PipelineMultiplex: 5,
			MaxFlushDelay:     50 * time.Microsecond,
			AlwaysPipelining:  true,
			DisableTCPNoDelay: true,
			DisableRetry:      true,
		},
	)
	return &Writer{
		client: client,
	}, err
}

func (w *Writer) Start(ctx context.Context, wg *sync.WaitGroup, data map[string][]byte, cols []string, sleep time.Duration) {
	defer wg.Done()
	for i := 0; ; i++ {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			logger.Get().Info("writer stopping due to context cancellation")
			return
		default:
		}

		err := hSetExpire(ctx, time.Second*1, w.client, fmt.Sprintf("%d", i), cols, data, time.Hour*1)
		if err != nil {
			// Check if error is due to context cancellation
			if ctx.Err() != nil {
				logger.Get().Info("writer stopping due to context cancellation", zap.Error(err))
				return
			}
			logger.Get().Error("unable to hSetExpire", zap.Error(err))
			break
		}
		if i%500 == 0 {
			logger.Get().Info("inserted", zap.Int("num", i))
		}

		// Use context-aware sleep
		select {
		case <-ctx.Done():
			logger.Get().Info("writer stopping due to context cancellation")
			return
		case <-time.After(sleep):
		}
	}
}

func hSetExpire(
	ctx context.Context,
	timeout time.Duration,
	client rueidis.Client,
	key string,
	cols []string,
	data map[string][]byte,
	ttl time.Duration,
) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	convertedSlice := make([]string, 0, len(cols)*2)
	for _, col := range cols {
		if _, ok := data[col]; !ok {
			return fmt.Errorf("field %s not found in data", col)
		}
		convertedSlice = append(convertedSlice, col, string(data[col]))
	}

	cmd := client.B().
		Arbitrary("HSETEXPIRE").
		Keys(key).
		Args(strconv.Itoa(int(ttl.Seconds()))).
		Args(convertedSlice...).
		Build()
	resp := client.Do(timeoutCtx, cmd)
	err := resp.Error()
	return err
}
