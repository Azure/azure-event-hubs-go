package eph

import (
	"context"
	"os"
	"os/signal"
	"sync"

	"github.com/Azure/azure-event-hubs-go"
	"github.com/Azure/azure-event-hubs-go/auth"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"pack.ag/amqp"
)

const (
	banner = `
    ______                 __  __  __      __
   / ____/   _____  ____  / /_/ / / /_  __/ /_  _____
  / __/ | | / / _ \/ __ \/ __/ /_/ / / / / __ \/ ___/
 / /___ | |/ /  __/ / / / /_/ __  / /_/ / /_/ (__  )
/_____/ |___/\___/_/ /_/\__/_/ /_/\__,_/_.___/____/

=> processing events, ctrl+c to exit
`
)

type (
	// EventProcessorHost provides functionality for coordinating and balancing load across multiple Event Hub partitions
	EventProcessorHost struct {
		namespace     string
		hubName       string
		name          string
		tokenProvider auth.TokenProvider
		client        eventhub.Client
		leaser        Leaser
		checkpointer  Checkpointer
		scheduler     *scheduler
		handlers      map[string]eventhub.Handler
		hostMu        sync.Mutex
		handlersMu    sync.Mutex
	}

	// EventProcessorHostOption provides configuration options for an EventProcessorHost
	EventProcessorHostOption func(host *EventProcessorHost) error

	// Receiver provides the ability to handle Event Hub events
	Receiver interface {
		Receive(ctx context.Context, handler eventhub.Handler) (close func() error, err error)
	}
)

// New constructs a new instance of an EventHostProcessor
func New(namespace, hubName string, tokenProvider auth.TokenProvider, leaser Leaser, checkpointer Checkpointer, opts ...EventProcessorHostOption) (*EventProcessorHost, error) {
	client, err := eventhub.NewClient(namespace, hubName, tokenProvider)
	if err != nil {
		return nil, err
	}

	host := &EventProcessorHost{
		namespace:     namespace,
		name:          uuid.NewV4().String(),
		hubName:       hubName,
		tokenProvider: tokenProvider,
		leaser:        leaser,
		checkpointer:  checkpointer,
		client:        client,
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
func (h *EventProcessorHost) Receive(ctx context.Context, handler eventhub.Handler) (close func() error, err error) {
	if err := h.setup(ctx); err != nil {
		return nil, err
	}

	h.handlersMu.Lock()
	defer h.handlersMu.Unlock()
	id := uuid.NewV4().String()
	h.handlers[id] = handler
	close = func() error {
		h.handlersMu.Lock()
		defer h.handlersMu.Unlock()

		delete(h.handlers, id)
		return nil
	}
	return close, nil
}

// Start begins processing of messages for registered handlers on the EventHostProcessor. The call is blocking.
func (h *EventProcessorHost) Start() error {
	log.Println(banner)
	go h.scheduler.Run()

	// Wait for a signal to quit:
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	_ <- signalChan

	log.Println("shutting down...")
	return h.scheduler.Stop()
}

// StartNonBlocking begins processing of messages for registered handlers
func (h *EventProcessorHost) StartNonBlocking() {
	log.Println(banner)
	go h.scheduler.Run()
}

func (h *EventProcessorHost) setup(ctx context.Context) error {
	h.hostMu.Lock()
	defer h.hostMu.Unlock()

	if h.scheduler == nil {
		if err := h.leaser.EnsureStore(ctx); err != nil {
			return err
		}

		if err := h.checkpointer.EnsureStore(ctx); err != nil {
			return err
		}

		scheduler, err := newScheduler(ctx, h)
		if err != nil {
			return err
		}
		h.scheduler = scheduler
	}
	return nil
}

func (h *EventProcessorHost) compositeHandlers() eventhub.Handler {
	return func(ctx context.Context, msg *amqp.Message) error {
		var wg sync.WaitGroup
		for _, handle := range h.handlers {
			wg.Add(1)
			go func(boundHandle eventhub.Handler) {
				if err := boundHandle(ctx, msg); err != nil {
					log.Error(err)
				}
				wg.Done()
			}(handle)
		}
		wg.Wait()
		return nil
	}
}

// Close stops the EventHostProcessor from processing messages
func (h *EventProcessorHost) Close(ctx context.Context) error {
	if h.scheduler != nil {
		if err := h.scheduler.Stop(); err != nil {
			log.Error(err)
			if h.client != nil {
				_ = h.client.Close()
			}
			return err
		}
	}

	if h.client != nil {
		return h.client.Close()
	}
	return nil
}
