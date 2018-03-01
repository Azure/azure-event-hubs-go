package eventhub

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/Azure/azure-event-hubs-go/aad"
	"github.com/Azure/azure-event-hubs-go/auth"
	"github.com/Azure/azure-event-hubs-go/mgmt"
	"github.com/Azure/azure-event-hubs-go/persist"
	"github.com/Azure/azure-event-hubs-go/sas"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"pack.ag/amqp"
)

const (
	maxUserAgentLen = 128
	rootUserAgent   = "/golang-event-hubs"
)

type (
	hub struct {
		name              string
		namespace         *namespace
		receivers         map[string]*receiver
		sender            *sender
		senderPartitionID *string
		receiverMu        sync.Mutex
		senderMu          sync.Mutex
		offsetPersister   persist.OffsetPersister
		userAgent         string
	}

	// Handler is the function signature for any receiver of AMQP messages
	Handler func(context.Context, *amqp.Message) error

	// Sender provides the ability to send a messages
	Sender interface {
		Send(ctx context.Context, message *amqp.Message, opts ...SendOption) error
	}

	// PartitionedReceiver provides the ability to receive messages from a given partition
	PartitionedReceiver interface {
		Receive(ctx context.Context, partitionID string, handler Handler, opts ...ReceiveOption) (ListenerHandle, error)
	}

	// Closer provides the ability to close a connection or client
	Closer interface {
		Close() error
	}

	// Manager provides the ability to query management node information about a node
	Manager interface {
		GetRuntimeInformation(context.Context) (*mgmt.HubRuntimeInformation, error)
		GetPartitionInformation(context.Context, string) (*mgmt.HubPartitionRuntimeInformation, error)
	}

	// Client provides the ability to send and receive Event Hub messages
	Client interface {
		Sender
		PartitionedReceiver
		Closer
		Manager
	}

	// HubOption provides structure for configuring new Event Hub instances
	HubOption func(h *hub) error
)

// NewClient creates a new Event Hub client for sending and receiving messages
func NewClient(namespace, name string, tokenProvider auth.TokenProvider, opts ...HubOption) (Client, error) {
	ns := newNamespace(namespace, tokenProvider, azure.PublicCloud)
	h := &hub{
		name:            name,
		namespace:       ns,
		offsetPersister: new(persist.MemoryPersister),
		userAgent:       rootUserAgent,
		receivers:       make(map[string]*receiver),
	}

	for _, opt := range opts {
		err := opt(h)
		if err != nil {
			return nil, err
		}
	}

	return h, nil
}

// NewClientWithNamespaceNameAndEnvironment creates a new Event Hub client for sending and receiving messages from
// environment variables with supplied namespace and name
func NewClientWithNamespaceNameAndEnvironment(namespace, name string, opts ...HubOption) (Client, error) {
	var provider auth.TokenProvider
	aadProvider, aadErr := aad.NewProviderFromEnvironment()
	sasProvider, sasErr := sas.NewProviderFromEnvironment()

	if aadErr != nil && sasErr != nil {
		// both failed
		log.Debug("both token providers failed")
		return nil, errors.Errorf("neither Azure Active Directory nor SAS token provider could be built - AAD error: %v, SAS error: %v", aadErr, sasErr)
	}

	if aadProvider != nil {
		log.Debug("using AAD provider")
		provider = aadProvider
	} else {
		log.Debug("using SAS provider")
		provider = sasProvider
	}

	h, err := NewClient(namespace, name, provider, opts...)
	if err != nil {
		return nil, err
	}

	return h, nil
}

// NewClientFromEnvironment creates a new Event Hub client for sending and receiving messages from environment variables
func NewClientFromEnvironment(opts ...HubOption) (Client, error) {
	const envErrMsg = "environment var %s must not be empty"
	var namespace, name string

	if namespace = os.Getenv("EVENTHUB_NAMESPACE"); namespace == "" {
		return nil, errors.Errorf(envErrMsg, "EVENTHUB_NAMESPACE")
	}

	if name = os.Getenv("EVENTHUB_NAME"); name == "" {
		return nil, errors.Errorf(envErrMsg, "EVENTHUB_NAME")
	}

	return NewClientWithNamespaceNameAndEnvironment(namespace, name, opts...)
}

// GetRuntimeInformation fetches runtime information from the Event Hub management node
func (h *hub) GetRuntimeInformation(ctx context.Context) (*mgmt.HubRuntimeInformation, error) {
	client := mgmt.NewClient(h.namespace.name, h.name, h.namespace.tokenProvider, h.namespace.environment)
	conn, err := h.namespace.connection()
	if err != nil {
		return nil, err
	}
	info, err := client.GetHubRuntimeInformation(ctx, conn)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// GetPartitionInformation fetches runtime information about a specific partition from the Event Hub management node
func (h *hub) GetPartitionInformation(ctx context.Context, partitionID string) (*mgmt.HubPartitionRuntimeInformation, error) {
	client := mgmt.NewClient(h.namespace.name, h.name, h.namespace.tokenProvider, h.namespace.environment)
	conn, err := h.namespace.connection()
	if err != nil {
		return nil, err
	}
	info, err := client.GetHubPartitionRuntimeInformation(ctx, conn, partitionID)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// Close drains and closes all of the existing senders, receivers and connections
func (h *hub) Close() error {
	var lastErr error
	for _, r := range h.receivers {
		if err := r.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Receive subscribes for messages sent to the provided entityPath.
func (h *hub) Receive(ctx context.Context, partitionID string, handler Handler, opts ...ReceiveOption) (ListenerHandle, error) {
	h.receiverMu.Lock()
	defer h.receiverMu.Unlock()

	receiver, err := h.newReceiver(ctx, partitionID, opts...)
	if err != nil {
		return nil, err
	}

	if r, ok := h.receivers[receiver.getIdentifier()]; ok {
		r.Close()
	}
	h.receivers[receiver.getIdentifier()] = receiver
	listenerContext := receiver.Listen(handler)

	return listenerContext, nil
}

// Send sends an AMQP message to the broker
func (h *hub) Send(ctx context.Context, message *amqp.Message, opts ...SendOption) error {
	sender, err := h.getSender(ctx)
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

// HubWithOffsetPersistence configures the hub instance to read and write offsets so that if a hub is interrupted, it
// can resume after the last consumed event.
func HubWithOffsetPersistence(offsetPersister persist.OffsetPersister) HubOption {
	return func(h *hub) error {
		h.offsetPersister = offsetPersister
		return nil
	}
}

// HubWithUserAgent configures the hub to append the given string to the user agent sent to the server
//
// This option can be specified multiple times to add additional segments.
//
// Max user agent length is specified by the const maxUserAgentLen.
func HubWithUserAgent(userAgent string) HubOption {
	return func(h *hub) error {
		return h.appendAgent(userAgent)
	}
}

// HubWithEnvironment configures the hub to use the specified environment.
//
// By default, the hub instance will use Azure US Public cloud environment
func HubWithEnvironment(env azure.Environment) HubOption {
	return func(h *hub) error {
		h.namespace.environment = env
		return nil
	}
}

func (h *hub) appendAgent(userAgent string) error {
	ua := path.Join(h.userAgent, userAgent)
	if len(ua) > maxUserAgentLen {
		return errors.Errorf("user agent string has surpassed the max length of %d", maxUserAgentLen)
	}
	h.userAgent = ua
	return nil
}

func (h *hub) getSender(ctx context.Context) (*sender, error) {
	h.senderMu.Lock()
	defer h.senderMu.Unlock()

	if h.sender == nil {
		s, err := h.newSender(ctx)
		if err != nil {
			return nil, err
		}
		h.sender = s
	}
	// add recover logic here
	return h.sender, nil
}
