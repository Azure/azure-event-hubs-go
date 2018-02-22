package eph

import (
	"context"
	"sync"

	"github.com/Azure/azure-event-hubs-go"
	"github.com/Azure/azure-event-hubs-go/auth"
	"github.com/satori/go.uuid"
)

type (
	EventProcessorHost struct {
		namespace     string
		hubName       string
		name          string
		tokenProvider auth.TokenProvider
		client        eventhub.Client
		leaser        Leaser
		checkpointer  Checkpointer
		hostMu        sync.Mutex
	}

	EventProcessorHostOption func(host *EventProcessorHost) error

	Receiver interface {
		Receive(ctx context.Context, handler eventhub.Handler) error
	}
)

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

func (h *EventProcessorHost) Receive(ctx context.Context, handler eventhub.Handler) error {
	if err := h.start(ctx); err != nil {
		return err
	}

}

func (h *EventProcessorHost) start(ctx context.Context) error {
	h.hostMu.Lock()
	defer h.hostMu.Unlock()

	if err := h.leaser.EnsureStore(ctx); err != nil {
		return err
	}

	if err := h.checkpointer.EnsureStore(ctx); err != nil {
		return err
	}
}

func (h *EventProcessorHost) Close(ctx context.Context) error {
	if h.client != nil {
		return h.client.Close()
	}
	return nil
}
