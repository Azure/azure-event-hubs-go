package cmd

import (
	"context"
	"crypto/rand"
	"time"

	"github.com/Azure/azure-event-hubs-go"
	"github.com/Azure/azure-event-hubs-go/sas"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	sendCmd.Flags().IntVar(&messageCount, "msg-count", 1, "number of messages to send")
	sendCmd.Flags().IntVar(&messageSize, "msg-size", 256, "size in bytes of each message")
	rootCmd.AddCommand(sendCmd)
}

var (
	messageCount, messageSize int

	sendCmd = &cobra.Command{
		Use:   "send",
		Short: "Send messages to an Event Hub",
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
			hub, err := eventhub.NewClient(namespace, hubName, provider)
			if err != nil {
				log.Error(err)
				return
			}
			events := make([]*eventhub.Event, messageCount)
			for i := 0; i < messageCount; i++ {
				data := make([]byte, messageSize)
				rand.Read(data)
				events[i] = eventhub.NewEvent(data)
			}

			for _, event := range events {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := hub.Send(ctx, event)
				if err != nil {
					log.Errorln(err)
				}
				cancel()
			}
		},
	}
)
