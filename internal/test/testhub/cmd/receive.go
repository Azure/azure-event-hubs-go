package cmd

//	MIT License
//
//	Copyright (c) Microsoft Corporation. All rights reserved.
//
//	Permission is hereby granted, free of charge, to any person obtaining a copy
//	of this software and associated documentation files (the "Software"), to deal
//	in the Software without restriction, including without limitation the rights
//	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//	copies of the Software, and to permit persons to whom the Software is
//	furnished to do so, subject to the following conditions:
//
//	The above copyright notice and this permission notice shall be included in all
//	copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//	SOFTWARE

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
