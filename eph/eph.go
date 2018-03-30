// Package eph provides functionality for balancing load of Event Hub receivers through scheduling receivers across
// processes and machines.
package eph

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
	"time"

	"github.com/Azure/azure-amqp-common-go/auth"
	"github.com/Azure/azure-amqp-common-go/log"
	"github.com/Azure/azure-amqp-common-go/persist"
	"github.com/Azure/azure-amqp-common-go/uuid"
	"github.com/Azure/azure-event-hubs-go"
	"github.com/opentracing/opentracing-go"
	tag "github.com/opentracing/opentracing-go/ext"
)

const (
	banner = `
    ______                 __  __  __      __
   / ____/   _____  ____  / /_/ / / /_  __/ /_  _____
  / __/ | | / / _ \/ __ \/ __/ /_/ / / / / __ \/ ___/
 / /___ | |/ /  __/ / / / /_/ __  / /_/ / /_/ (__  )
/_____/ |___/\___/_/ /_/\__/_/ /_/\__,_/_.___/____/

`

	exitPrompt = "=> processing events, ctrl+c to exit"
)

type (
	// EventProcessorHost provides functionality for coordinating and balancing load across multiple Event Hub partitions
	EventProcessorHost struct {
		namespace     string
		hubName       string
		name          string
		tokenProvider auth.TokenProvider
		client        *eventhub.Hub
		leaser        Leaser
		checkpointer  Checkpointer
		scheduler     *scheduler
		handlers      map[string]eventhub.Handler
		hostMu        sync.Mutex
		handlersMu    sync.Mutex
		partitionIDs  []string
	}

	// EventProcessorHostOption provides configuration options for an EventProcessorHost
	EventProcessorHostOption func(host *EventProcessorHost) error

	// Receiver provides the ability to handle Event Hub events
	Receiver interface {
		Receive(ctx context.Context, handler eventhub.Handler) (close func() error, err error)
	}

	checkpointPersister struct {
		checkpointer Checkpointer
	}
)

// New constructs a new instance of an EventHostProcessor
func New(ctx context.Context, namespace, hubName string, tokenProvider auth.TokenProvider, leaser Leaser, checkpointer Checkpointer, opts ...EventProcessorHostOption) (*EventProcessorHost, error) {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.New")
	defer span.Finish()

	persister := checkpointPersister{checkpointer: checkpointer}
	client, err := eventhub.NewHub(namespace, hubName, tokenProvider, eventhub.HubWithOffsetPersistence(persister))
	if err != nil {
		return nil, err
	}

	runtimeInfo, err := client.GetRuntimeInformation(ctx)
	if err != nil {
		return nil, err
	}

	hostName, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	host := &EventProcessorHost{
		namespace:     namespace,
		name:          hostName.String(),
		hubName:       hubName,
		tokenProvider: tokenProvider,
		client:        client,
		handlers:      make(map[string]eventhub.Handler),
		leaser:        leaser,
		checkpointer:  checkpointer,
		partitionIDs:  runtimeInfo.PartitionIDs,
	}

	for _, opt := range opts {
		err := opt(host)
		if err != nil {
			return nil, err
		}
	}

	return host, nil
}

// Receive provides the ability to register a handler for processing Event Hub messages
func (h *EventProcessorHost) Receive(handler eventhub.Handler) (close func() error, err error) {
	h.handlersMu.Lock()
	defer h.handlersMu.Unlock()

	receiverID, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	h.handlers[receiverID.String()] = handler
	close = func() error {
		h.handlersMu.Lock()
		defer h.handlersMu.Unlock()

		delete(h.handlers, receiverID.String())
		return nil
	}
	return close, nil
}

// Start begins processing of messages for registered handlers on the EventHostProcessor. The call is blocking.
func (h *EventProcessorHost) Start(ctx context.Context) error {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.EventProcessorHost.Start")
	defer span.Finish()

	fmt.Print(banner)
	fmt.Println(exitPrompt)
	if err := h.setup(ctx); err != nil {
		return err
	}
	go h.scheduler.Run()

	// Wait for a signal to quit:
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	<-signalChan
	return h.Close(ctx)
}

// StartNonBlocking begins processing of messages for registered handlers
func (h *EventProcessorHost) StartNonBlocking(ctx context.Context) error {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.EventProcessorHost.StartNonBlocking")
	defer span.Finish()

	fmt.Print(banner)
	if err := h.setup(ctx); err != nil {
		return err
	}
	go h.scheduler.Run()
	return nil
}

// GetName returns the name of the EventProcessorHost
func (h *EventProcessorHost) GetName() string {
	return h.name
}

// GetPartitionIDs fetches the partition IDs for the Event Hub
func (h *EventProcessorHost) GetPartitionIDs() []string {
	return h.partitionIDs
}

// PartitionIDsBeingProcessed returns the partition IDs currently receiving messages
func (h *EventProcessorHost) PartitionIDsBeingProcessed() []string {
	ids := make([]string, len(h.scheduler.receivers))
	count := 0
	for key := range h.scheduler.receivers {
		ids[count] = key
		count++
	}
	return ids
}

// Close stops the EventHostProcessor from processing messages
func (h *EventProcessorHost) Close(ctx context.Context) error {
	fmt.Println("shutting down...")
	if h.scheduler != nil {
		if err := h.scheduler.Stop(ctx); err != nil {
			if h.client != nil {
				_ = h.client.Close(ctx)
			}
			return err
		}
	}

	if h.leaser != nil {
		_ = h.leaser.Close()
	}

	if h.checkpointer != nil {
		_ = h.checkpointer.Close()
	}

	return h.client.Close(ctx)
}

func (h *EventProcessorHost) setup(ctx context.Context) error {
	h.hostMu.Lock()
	defer h.hostMu.Unlock()
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.EventProcessorHost.setup")
	defer span.Finish()

	if h.scheduler == nil {
		h.leaser.SetEventHostProcessor(h)
		h.checkpointer.SetEventHostProcessor(h)
		if err := h.leaser.EnsureStore(ctx); err != nil {
			return err
		}

		if err := h.checkpointer.EnsureStore(ctx); err != nil {
			return err
		}

		scheduler := newScheduler(h)

		for _, partitionID := range h.partitionIDs {
			h.leaser.EnsureLease(ctx, partitionID)
			h.checkpointer.EnsureCheckpoint(ctx, partitionID)
		}

		h.scheduler = scheduler
	}
	return nil
}

func (h *EventProcessorHost) compositeHandlers() eventhub.Handler {
	return func(ctx context.Context, event *eventhub.Event) error {
		var wg sync.WaitGroup
		for _, handle := range h.handlers {
			wg.Add(1)
			go func(boundHandle eventhub.Handler) {
				if err := boundHandle(ctx, event); err != nil {
					log.For(ctx).Error(err)
				}
				wg.Done()
			}(handle)
		}
		wg.Wait()
		return nil
	}
}

func (c checkpointPersister) Write(namespace, name, consumerGroup, partitionID string, checkpoint persist.Checkpoint) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return c.checkpointer.UpdateCheckpoint(ctx, partitionID, checkpoint)
}

func (c checkpointPersister) Read(namespace, name, consumerGroup, partitionID string) (persist.Checkpoint, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return c.checkpointer.EnsureCheckpoint(ctx, partitionID)
}

func startConsumerSpanFromContext(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
	eventhub.ApplyComponentInfo(span)
	tag.SpanKindRPCClient.Set(span)
	return span, ctx
}
