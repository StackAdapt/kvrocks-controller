package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/redis/rueidis"
	"go.uber.org/zap"
)

func main() {
	numOfWriters := 1000
	writers := make([]*Writer, numOfWriters)
	for i := 0; i < numOfWriters; i++ {
		writer, err := NewWriter()
		if err != nil {
			logger.Get().Error("unable to get rueidis client", zap.Error(err))
			return
		}
		writers[i] = writer
	}

	ctx, cancel := context.WithCancel(context.Background())

	payload := []byte("123123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789456789")
	data := make(map[string][]byte)
	cols := []string{}
	for i := 0; i < 100; i++ {
		data[fmt.Sprintf("%d", i)] = payload
		cols = append(cols, fmt.Sprintf("%d", i))
	}

	for _, writer := range writers {
		writer.Start(ctx, data, cols, time.Second)
	}

	fmt.Println("Please enter some text and press Enter:")
	reader := bufio.NewReader(os.Stdin)
	// ReadString reads until the first occurrence of the delimiter ('\n' for Enter)
	// It returns the string read and an error, if any.
	_, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("error reading string")
	}
	// Print the input received
	fmt.Printf("exiting...")
	cancel()
}

type Writer struct {
	client rueidis.Client
}

func NewWriter() (*Writer, error) {
	client, err := rueidis.NewClient(
		rueidis.ClientOption{
			InitAddress:       []string{"127.0.0.1:7770"},
			ShuffleInit:       true,
			ConnWriteTimeout:  time.Millisecond * 300,
			DisableCache:      true, // client cache is not enabled on kvrocks
			PipelineMultiplex: 10,
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

func (w *Writer) Start(ctx context.Context, data map[string][]byte, cols []string, sleep time.Duration) error {
	for i := 0; ; i++ {
		err := hSetExpire(ctx, time.Second*1, w.client, fmt.Sprintf("hello:%d", i), cols, data, time.Hour*24)
		if err != nil {
			logger.Get().Error("unable to hSetExpire", zap.Error(err))
		}
		if i%10000 == 0 {
			logger.Get().Info("inserted", zap.Int("num", i))
		}
	}
	return nil
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
