package cmd

import (
	"context"
	"time"

	"github.com/Azure/azure-event-hubs-go"
	"github.com/Azure/azure-event-hubs-go/sas"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(receiveCmd)
}

var receiveCmd = &cobra.Command{
	Use:   "receive",
	Short: "Receive messages from an Event Hub",
	Args: func(cmd *cobra.Command, args []string) error {
		return checkAuthFlags()
	},
	Run: func(cmd *cobra.Command, args []string) {
		provider, err := sas.NewTokenProvider(sas.TokenProviderWithNamespaceAndKey(namespace, sasKeyName, sasKey))
		if err != nil {
			log.Error(err)
			return
		}
		hub, err := eventhub.NewClient(namespace, hubName, provider)
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

		messageCounts := make(map[string]int64)
		for _, partitionID := range runtimeInfo.PartitionIDs {
			messageCounts[partitionID] = 0
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := hub.Receive(ctx, partitionID, func(ctx context.Context, event *eventhub.Event) error {
				messageCounts[partitionID] = messageCounts[partitionID] + 1
				log.Printf("message count of %d for partition %q", messageCounts[partitionID], partitionID)
				return nil
			}, eventhub.ReceiveWithPrefetchCount(1000))
			cancel()
			if err != nil {
				log.Errorln(err)
				return
			}
		}
	},
}
