package eventhub

import (
	"context"
	"fmt"
	"pack.ag/amqp"
)

type (
	hub struct {
		name      string
		namespace *Namespace
	}

	// Handler is the function signature for any receiver of AMQP messages
	Handler func(context.Context, *amqp.Message) error

	// Sender provides the ability to send a messages
	Sender interface {
		Send(ctx context.Context, message *amqp.Message, opts ...SendOption) error
		SendBatch(ctx context.Context, mesages []*amqp.Message, opts ...SendOption) error
	}

	// Receiver provides the ability to receive messages
	Receiver interface {
		Receive(entityPath string, handler Handler, opts ...ReceiverOptions) error
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

	// ReceiverOptions provides a structure for configuring receivers
	ReceiverOptions func(receiver *receiver) error

	// TODO: make this real
	receiver struct{}
)

// Close drains and closes all of the existing senders, receivers and connections
func (h *hub) Close() error {
	return nil
}

// Listen subscribes for messages sent to the provided entityPath.
func (h *hub) Receive(entityPath string, handler Handler, opts ...ReceiverOptions) error {
	return fmt.Errorf("not implemented")
}

// Send sends an AMQP message to the broker
func (h *hub) Send(ctx context.Context, message *amqp.Message, opts ...SendOption) error {
	return fmt.Errorf("not implemented")
}

// Send sends a batch of AMQP message to the broker
func (h *hub) SendBatch(ctx context.Context, messages []*amqp.Message, opts ...SendOption) error {
	return fmt.Errorf("not implemented")
}
