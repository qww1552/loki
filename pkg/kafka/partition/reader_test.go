package partition

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/loki/v3/pkg/kafka"
	"github.com/grafana/loki/v3/pkg/kafka/testkafka"
	"github.com/grafana/loki/v3/pkg/logproto"
)

type mockConsumer struct {
	mock.Mock
	recordsChan chan []Record
	wg          sync.WaitGroup
}

func newMockConsumer() *mockConsumer {
	return &mockConsumer{
		recordsChan: make(chan []Record, 100),
	}
}

func (m *mockConsumer) Start(ctx context.Context, recordsChan <-chan []Record) func() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case records, ok := <-recordsChan:
				if !ok {
					return
				}
				m.recordsChan <- records
			}
		}
	}()
	return m.wg.Wait
}

type mockSlowConsumer struct {
	mock.Mock
	recordsChan chan []Record
	controlChan <-chan struct{}
	wg          sync.WaitGroup
}

func newMockSlowConsumer(controlChan <-chan struct{}) *mockSlowConsumer {
	return &mockSlowConsumer{
		recordsChan: make(chan []Record, 100),
		controlChan: controlChan,
	}
}

func (m *mockSlowConsumer) Start(ctx context.Context, recordsChan <-chan []Record) func() {
	start := time.Now()
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Context done", time.Since(start))
				return
			case <-m.controlChan:
				fmt.Println("Control chan fired", time.Since(start))
				records := <-recordsChan
				fmt.Println("Writing records", len(records), time.Since(start))
				m.recordsChan <- records
			}
		}
	}()
	return m.wg.Wait
}

func (m *mockConsumer) Flush(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func TestPartitionReader_BasicFunctionality(t *testing.T) {
	_, kafkaCfg := testkafka.CreateCluster(t, 1, "test-topic")
	consumer := newMockConsumer()

	consumerFactory := func(_ Committer) (Consumer, error) {
		return consumer, nil
	}

	partitionReader, err := NewReader(kafkaCfg, 0, "test-consumer-group", consumerFactory, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	producer, err := kafka.NewWriterClient(kafkaCfg, 100, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	err = services.StartAndAwaitRunning(context.Background(), partitionReader)
	require.NoError(t, err)

	stream := logproto.Stream{
		Labels:  labels.FromStrings("foo", "bar").String(),
		Entries: []logproto.Entry{{Timestamp: time.Now(), Line: "test"}},
	}

	records, err := kafka.Encode(0, "test-tenant", stream, 10<<20)
	require.NoError(t, err)
	require.Len(t, records, 1)

	producer.ProduceSync(context.Background(), records...)
	producer.ProduceSync(context.Background(), records...)

	// Wait for records to be processed
	assert.Eventually(t, func() bool {
		return len(consumer.recordsChan) == 2
	}, 10*time.Second, 100*time.Millisecond)

	// Verify the records
	for i := 0; i < 2; i++ {
		select {
		case receivedRecords := <-consumer.recordsChan:
			require.Len(t, receivedRecords, 1)
			assert.Equal(t, "test-tenant", receivedRecords[0].TenantID)
			assert.Equal(t, records[0].Value, receivedRecords[0].Content)
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for records")
		}
	}

	err = services.StopAndAwaitTerminated(context.Background(), partitionReader)
	require.NoError(t, err)
}

func TestPartitionReader_ProcessCatchUpAtStartup(t *testing.T) {
	_, kafkaCfg := testkafka.CreateCluster(t, 1, "test-topic")
	var consumerStarting *mockConsumer

	consumerFactory := func(_ Committer) (Consumer, error) {
		// Return two consumers to ensure we are processing requests during service `start()` and not during `run()`.
		if consumerStarting == nil {
			consumerStarting = newMockConsumer()
			return consumerStarting, nil
		}
		return newMockConsumer(), nil
	}

	partitionReader, err := NewReader(kafkaCfg, 0, "test-consumer-group", consumerFactory, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	producer, err := kafka.NewWriterClient(kafkaCfg, 100, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	stream := logproto.Stream{
		Labels:  labels.FromStrings("foo", "bar").String(),
		Entries: []logproto.Entry{{Timestamp: time.Now(), Line: "test"}},
	}

	records, err := kafka.Encode(0, "test-tenant", stream, 10<<20)
	require.NoError(t, err)
	require.Len(t, records, 1)

	producer.ProduceSync(context.Background(), records...)
	producer.ProduceSync(context.Background(), records...)

	// Enable the catch up logic so starting the reader will read any existing records.
	kafkaCfg.TargetConsumerLagAtStartup = time.Second * 1
	kafkaCfg.MaxConsumerLagAtStartup = time.Second * 2

	err = services.StartAndAwaitRunning(context.Background(), partitionReader)
	require.NoError(t, err)

	// This message should not be processed by the startingConsumer
	producer.ProduceSync(context.Background(), records...)

	// Wait for records to be processed
	require.Eventually(t, func() bool {
		return len(consumerStarting.recordsChan) == 1 // All pending messages will be received in one batch
	}, 10*time.Second, 10*time.Millisecond)

	receivedRecords := <-consumerStarting.recordsChan
	require.Len(t, receivedRecords, 2)
	assert.Equal(t, "test-tenant", receivedRecords[0].TenantID)
	assert.Equal(t, records[0].Value, receivedRecords[0].Content)
	assert.Equal(t, "test-tenant", receivedRecords[1].TenantID)
	assert.Equal(t, records[0].Value, receivedRecords[1].Content)

	assert.Equal(t, 0, len(consumerStarting.recordsChan))

	err = services.StopAndAwaitTerminated(context.Background(), partitionReader)
	require.NoError(t, err)
}

func TestPartitionReader_ProcessCommits(t *testing.T) {
	_, kafkaCfg := testkafka.CreateCluster(t, 1, "test-topic")
	controlChan := make(chan struct{})
	consumer := newMockSlowConsumer(controlChan)

	consumerFactory := func(_ Committer) (Consumer, error) {
		return consumer, nil
	}

	partitionReader, err := NewReader(kafkaCfg, 0, "test-consumer-group", consumerFactory, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	producer, err := kafka.NewWriterClient(kafkaCfg, 100, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	// Init the client: This usually happens in "start" but we want to manage our own lifecycle for this test.
	partitionReader.client, err = kafka.NewReaderClient(kafkaCfg, nil, log.NewNopLogger(),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			kafkaCfg.Topic: {0: kgo.NewOffset().AtStart()},
		}),
	)
	require.NoError(t, err)

	stream := logproto.Stream{
		Labels:  labels.FromStrings("foo", "bar").String(),
		Entries: []logproto.Entry{{Timestamp: time.Now(), Line: "test"}},
	}

	records, err := kafka.Encode(0, "test-tenant", stream, 10<<20)
	require.NoError(t, err)
	require.Len(t, records, 1)

	producer.ProduceSync(context.Background(), records...)
	producer.ProduceSync(context.Background(), records...)

	targetLag := time.Second * 1
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(targetLag*2))
	recordsChan := make(chan []Record)
	wait := consumer.Start(ctx, recordsChan)
	// Delay the reads from kafka by longer than the target lag, this triggers fetches to retry until we are caught up.
	kafkaReads := 0
	time.AfterFunc(targetLag+time.Millisecond*100, func() {
		for {
			if ctx.Err() != nil {
				return
			}
			controlChan <- struct{}{}
			kafkaReads++
		}
	})
	_, err = partitionReader.processNextFetchesUntilLagHonored(ctx, targetLag, log.NewNopLogger(), recordsChan)
	assert.NoError(t, err)
	// We should have read 3 times, the first read will be longer than maxLag, triggering the second, then a third to be sure we are caught up.
	assert.Equal(t, 3, kafkaReads)

	cancel()
	close(recordsChan)
	wait()
}
