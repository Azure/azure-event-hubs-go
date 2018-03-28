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
	"crypto/rand"
	"fmt"
	"time"

	"github.com/Azure/azure-amqp-common-go/sas"
	"github.com/Azure/azure-event-hubs-go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	sendCmd.Flags().IntVar(&messageCount, "msg-count", 10, "number of messages to send")
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
			hub, err := eventhub.NewHub(namespace, hubName, provider, eventhub.HubWithEnvironment(environment()))
			if err != nil {
				log.Error(err)
				return
			}

			log.Println(fmt.Sprintf("attempting to send %d messages", messageCount))
			sentMsgs := 0
			for i := 0; i < messageCount; i++ {
				data := make([]byte, messageSize)
				_, err := rand.Read(data)
				if err != nil {
					log.Errorln("unable to generate random bits for message")
					continue
				}
				event := eventhub.NewEvent(data)

				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
				err = hub.Send(ctx, event)
				if err != nil {
					log.Errorln(fmt.Sprintf("failed sending idx: %d", i), err)
				} else {
					sentMsgs++
				}
				cancel()
			}

			log.Printf("sent %d messages\n", sentMsgs)
		},
	}
)
