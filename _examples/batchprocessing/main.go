package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/azure-event-hubs-go/v3/persist"
)

func main() {
	ctx := context.Background()

	fp, err := persist.NewFilePersister(os.Getenv("EVENTHUB_FILEPERSIST_DIRECTORY"))
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	output, err := NewBatchWriter(fp, os.Stdout)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	hub, err := eventhub.NewHubFromEnvironment(eventhub.HubWithOffsetPersistence(output))
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	defer hub.Close(ctx)

	consumerGroup := os.Getenv("EVENTHUB_CONSUMERGROUP")
	if consumerGroup == "" {
		consumerGroup = "$Default"
	}
	runtimeInfo, err := hub.GetRuntimeInformation(ctx)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	partitionIds := runtimeInfo.PartitionIDs
	for _, partitionId := range partitionIds {
		_, err = hub.Receive(ctx, partitionId, output.HandleEvent, eventhub.ReceiveWithConsumerGroup(consumerGroup), eventhub.ReceiveWithPrefetchCount(20000))
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	<-signalChan
}
