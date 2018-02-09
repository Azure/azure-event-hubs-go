package eventhub

import (
	"context"
	"fmt"
	"pack.ag/amqp"
	"sync"
)

type (
	hub struct {
		name              string
		namespace         *Namespace
		receivers         []*receiver
		sender            *sender
		senderPartitionID *string
		receiverMu        sync.Mutex
		senderMu          sync.Mutex
	}

	// Handler is the function signature for any receiver of AMQP messages
	Handler func(context.Context, *amqp.Message) error

	// Sender provides the ability to send a messages
	Sender interface {
		Send(ctx context.Context, message *amqp.Message, opts ...SendOption) error
		//SendBatch(ctx context.Context, mesages []*amqp.Message, opts ...SendOption) error
	}

	// Receiver provides the ability to receive messages
	Receiver interface {
		Receive(partitionID string, handler Handler, opts ...ReceiveOption) error
	}

	// Closer provides the ability to close a connection or client
	Closer interface {
		Close() error
	}

	// SenderReceiver provides the ability to send and receive AMQP messages
	SenderReceiver interface {
		Sender
		Receiver
		Closer
	}

	// HubOption provides structure for configuring new Event Hub instances
	HubOption func(h *hub) error
)

// Close drains and closes all of the existing senders, receivers and connections
func (h *hub) Close() error {
	return nil
}

// Listen subscribes for messages sent to the provided entityPath.
func (h *hub) Receive(partitionID string, handler Handler, opts ...ReceiveOption) error {
	h.receiverMu.Lock()
	defer h.receiverMu.Unlock()

	receiver, err := h.newReceiver(partitionID, opts...)
	if err != nil {
		return err
	}

	h.receivers = append(h.receivers, receiver)
	receiver.Listen(handler)
	return nil
}

// Send sends an AMQP message to the broker
func (h *hub) Send(ctx context.Context, message *amqp.Message, opts ...SendOption) error {
	sender, err := h.getSender()
	if err != nil {
		return err
	}
	return sender.Send(ctx, message, opts...)
}

// Send sends a batch of AMQP message to the broker
func (h *hub) SendBatch(ctx context.Context, messages []*amqp.Message, opts ...SendOption) error {
	return fmt.Errorf("not implemented")
}

// HubWithPartitionedSender configures the hub instance to send to a specific event hub partition
func HubWithPartitionedSender(partitionID string) HubOption {
	return func(h *hub) error {
		h.senderPartitionID = &partitionID
		return nil
	}
}

func (h *hub) getSender() (*sender, error) {
	h.senderMu.Lock()
	defer h.senderMu.Unlock()

	if h.sender == nil {
		s, err := h.newSender()
		if err != nil {
			return nil, err
		}
		h.sender = s
	}
	// add recover logic here
	return h.sender, nil
}
