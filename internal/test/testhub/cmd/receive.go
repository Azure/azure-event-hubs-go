package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-amqp-common-go/sas"
	"github.com/Azure/azure-event-hubs-go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type (
	messageHandler struct {
		counter     int64
		partitionID string
	}
)

func init() {
	rootCmd.AddCommand(receiveCmd)
}

var (
	mu sync.Mutex

	receiveCmd = &cobra.Command{
		Use:   "receive",
		Short: "Receive messages from an Event Hub",
		Args: func(cmd *cobra.Command, args []string) error {
			if debug {
				log.SetLevel(log.DebugLevel)
			}
			return checkAuthFlags()
		},
		Run: func(cmd *cobra.Command, args []string) {
			provider, err := sas.NewTokenProvider(sas.TokenProviderWithNamespaceAndKey(namespace, sasKeyName, sasKey))
			if err != nil {
				log.Error(err)
				return
			}
			hub, err := eventhub.NewHub(namespace, hubName, provider, eventhub.HubWithEnvironment(environment()))
			if err != nil {
				log.Error(err)
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			runtimeInfo, err := hub.GetRuntimeInformation(ctx)
			cancel()
			if err != nil {
				log.Errorln(err)
				return
			}

			handlers := make([]messageHandler, len(runtimeInfo.PartitionIDs))
			for idx, partitionID := range runtimeInfo.PartitionIDs {
				handlers[idx] = messageHandler{partitionID: partitionID}
			}

			for idx, partitionID := range runtimeInfo.PartitionIDs {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, err := hub.Receive(
					ctx,
					partitionID,
					handlers[idx].handle,
					eventhub.ReceiveWithPrefetchCount(1000),
					eventhub.ReceiveWithLatestOffset())
				cancel()
				if err != nil {
					log.Errorln(err)
					return
				}
			}

			// Wait for a signal to quit:
			fmt.Println("=> ctrl+c to exit")
			signalChan := make(chan os.Signal, 1)
			signal.Notify(signalChan, os.Interrupt, os.Kill)
			<-signalChan
			hub.Close()
			return
		},
	}
)

func (m *messageHandler) handle(ctx context.Context, event *eventhub.Event) error {
	atomic.AddInt64(&m.counter, 1)
	msg := fmt.Sprintf("message count of %d for partition %q", m.counter, m.partitionID)
	log.Println(msg)
	return nil
}
