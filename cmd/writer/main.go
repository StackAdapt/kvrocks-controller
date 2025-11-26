package main

import (
	"context"
	"flag"
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
	// Define command-line flags
	numWriters := flag.Int("writers", runtime.GOMAXPROCS(0), "number of writer goroutines")
	writeDelay := flag.Duration("delay", 0, "delay between writes (e.g., 1ms, 100us)")
	start := flag.Int("start", 0, "index to start at")

	flag.Parse()

	numOfWriters := *numWriters
	if numOfWriters <= 0 {
		numOfWriters = runtime.GOMAXPROCS(0)
		logger.Get().Warn("invalid number of writers, using default", zap.Int("default", numOfWriters))
	}

	logger.Get().Info("starting service", zap.Int("writers", numOfWriters), zap.Duration("delay", *writeDelay), zap.Int("start_index", *start))

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
	payload := []byte("11123123456789123456712312345678912123123456789123456712312345678912345671231234567891234567123123456789123456734567123123456789123456712312345678912345672312345678912345671231234567891212312345678912345671231234567891234567123123456789123456712312345678912345673456712312345678912345671231234567891234567123123456789123456712312345678912123123456789123456712312345678912345671231234567891234567123123456789123456734567123123456789123456712312345678912345672312345678912345671231234567891212312345678912345671231234567891234567123123456789123456712312345678912345673456712312345678912345671231234567891234567")
	payloadSize := len(payload)
	data := make(map[string][]byte)
	cols := []string{}
	for i := 0; i < 300; i++ {
		data[fmt.Sprintf("%d", i)] = payload
		cols = append(cols, fmt.Sprintf("%d", i))
	}
	// Calculate total size per key: payload size * number of fields
	totalSizePerKey := payloadSize * len(cols)
	logger.Get().Info("payload configured",
		zap.Int("payload_size_bytes", payloadSize),
		zap.Int("num_fields", len(cols)),
		zap.Int("total_size_per_key_bytes", totalSizePerKey),
	)

	var wg sync.WaitGroup

	logger.Get().Info("starting writers", zap.Int("start_index", *start))
	for i, writer := range writers {
		wg.Add(1)
		go writer.Start(ctx, &wg, data, cols, *writeDelay, int64(*start), i, numOfWriters)
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
			ConnWriteTimeout:  10 * time.Second,
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

func (w *Writer) Start(ctx context.Context, wg *sync.WaitGroup, data map[string][]byte, cols []string, sleep time.Duration, startIndex int64, writerIndex int, numWriters int) {
	defer wg.Done()
	// Each writer starts at startIndex + writerIndex, then increments by numWriters
	// This ensures no two writers write to the same key
	keyIndex := startIndex + int64(writerIndex)
	iteration := int64(0)
	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			logger.Get().Info("writer stopping due to context cancellation", zap.Int("writer", writerIndex))
			return
		default:
		}

		// Convert integer keyIndex to alphabet-only key before passing to hSetExpire
		alphabetKey := intToAlphabetKey(keyIndex)
		err := hSetExpire(ctx, time.Millisecond*1000, w.client, alphabetKey, cols, data, time.Hour*24*7)
		if err != nil {
			// Check if error is due to context cancellation
			if ctx.Err() != nil {
				logger.Get().Info("writer stopping due to context cancellation", zap.Int("writer", writerIndex), zap.Error(err))
				return
			}
			logger.Get().Error("unable to hSetExpire", zap.Error(err), zap.Int("writer", writerIndex), zap.Int64("key", keyIndex))
		}
		if iteration%5000 == 0 {
			alphabetKey := intToAlphabetKey(keyIndex)
			logger.Get().Info("inserted", zap.Int("writer", writerIndex), zap.Int64("keyIndex", keyIndex), zap.String("key", alphabetKey), zap.Int64("iteration", iteration))
		}

		// Increment to next key for this writer (offset by numWriters)
		iteration++
		keyIndex = startIndex + int64(writerIndex) + (iteration * int64(numWriters))

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
