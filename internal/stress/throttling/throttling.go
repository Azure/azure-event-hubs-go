package main

import (
	"context"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/azure-event-hubs-go/v3/internal/stress"
	"github.com/devigned/tab"
	"github.com/joho/godotenv"
)

var SenderMaxRetryCount = 10
var MaxBatches = 100

func main() {
	godotenv.Load("../../../.env")
	cs := os.Getenv("EVENTHUB_CONNECTION_STRING")

	hub, err := eventhub.NewHubFromConnectionString(cs, eventhub.HubWithSenderMaxRetryCount(SenderMaxRetryCount))

	if err != nil {
		log.Fatalf("Failed to create hub: %s", err.Error())
	}

	startSequenceNumbers := getPartitionCounts(context.Background(), hub)

	// Generate some large batches of messages and send them in parallel.
	// The Go SDK is fast enough that this will cause a 1TU instance to throttle
	// us, allowing you to see how our code reacts to it.
	tab.Register(&stress.StderrTracer{NoOpTracer: &tab.NoOpTracer{}})
	lastExpectedId := sendMessages(hub)

	log.Printf("Sending complete, last expected ID = %d", lastExpectedId)

	endSequenceNumbers := getPartitionCounts(context.Background(), hub)

	for partitionID, endSequenceNumber := range endSequenceNumbers {
		startSequenceNumber := startSequenceNumbers[partitionID]

		log.Printf("[%s] diff: %d", partitionID, endSequenceNumber-startSequenceNumber)
	}
}

func sendMessages(hub *eventhub.Hub) int64 {
	var batches []eventhub.BatchIterator
	nextTestId := int64(0)

	log.Printf("Creating event batches")

	for i := 0; i < MaxBatches; i++ {
		batches = append(batches, createEventBatch(&nextTestId))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	wg := &sync.WaitGroup{}

	log.Printf("Sending event batches")

	var totalBatches int64 = 0

	for i, batch := range batches {
		wg.Add(1)

		go func(idx int, batch eventhub.BatchIterator) {
			err := hub.SendBatch(ctx, batch)

			if err != nil {
				log.Fatalf("ERROR sending batch: %s", err.Error())
			}

			wg.Done()
			atomic.AddInt64(&totalBatches, 1)
			log.Printf("[%d/%d] sent...", totalBatches, len(batches))
		}(i, batch)
	}

	wg.Wait()

	return nextTestId - 1
}

func createEventBatch(testId *int64) eventhub.BatchIterator {
	var events []*eventhub.Event
	var data = [1024]byte{1}

	// simple minimum
	batchSize := 880

	for i := 0; i < batchSize; i++ {
		events = append(events, &eventhub.Event{
			Data: data[:],
			Properties: map[string]interface{}{
				"testId": *testId,
			},
		})

		*testId++
	}

	return eventhub.NewEventBatchIterator(events...)
}

func getPartitionCounts(ctx context.Context, hub *eventhub.Hub) map[string]int64 {
	sequenceNumbers := map[string]int64{}

	runtimeInfo, err := hub.GetRuntimeInformation(ctx)

	if err != nil {
		log.Fatalf("Failed to get runtime information from hub: %s", err.Error())
	}

	for _, partitionId := range runtimeInfo.PartitionIDs {
		partInfo, err := hub.GetPartitionInformation(ctx, partitionId)

		if err != nil {
			log.Fatalf("Failed to get partition info for partition ID %s: %s", partitionId, err.Error())
		}

		sequenceNumbers[partitionId] = partInfo.LastSequenceNumber
	}

	return sequenceNumbers
}
