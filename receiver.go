package eventhub

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-event-hubs-go/mgmt"
	"github.com/Azure/azure-event-hubs-go/persist"
	log "github.com/sirupsen/logrus"
	"pack.ag/amqp"
)

const (
	// DefaultConsumerGroup is the default name for a event stream consumer group
	DefaultConsumerGroup = "$Default"

	offsetAnnotationName = "x-opt-offset"

	amqpAnnotationFormat = "amqp.annotation.%s >%s '%s'"

	defaultPrefetchCount = 100

	epochKey = mgmt.MsftVendor + ":epoch"
)

// receiver provides session and link handling for a receiving entity path
type (
	receiver struct {
		hub           *hub
		session       *session
		receiver      *amqp.Receiver
		consumerGroup string
		partitionID   string
		prefetchCount uint32
		done          func()
		epoch         *int64
		lastError     error
	}

	// ReceiveOption provides a structure for configuring receivers
	ReceiveOption func(receiver *receiver) error

	// ListenerHandle provides a way to manage the lifespan of a listener
	ListenerHandle interface {
		Done() <-chan struct{}
		Err() error
		Close() error
	}

	listenerHandle struct {
		r   *receiver
		ctx context.Context
	}
)

// ReceiveWithConsumerGroup configures the receiver to listen to a specific consumer group
func ReceiveWithConsumerGroup(consumerGroup string) ReceiveOption {
	return func(receiver *receiver) error {
		receiver.consumerGroup = consumerGroup
		return nil
	}
}

// ReceiveWithStartingOffset configures the receiver to start at a given position in the event stream
func ReceiveWithStartingOffset(offset string) ReceiveOption {
	return func(receiver *receiver) error {
		receiver.storeLastReceivedOffset(persist.NewCheckpoint(offset, 0, time.Time{}))
		return nil
	}
}

// ReceiveWithLatestOffset configures the receiver to start at a given position in the event stream
func ReceiveWithLatestOffset() ReceiveOption {
	return func(receiver *receiver) error {
		receiver.storeLastReceivedOffset(persist.NewCheckpointFromEndOfStream())
		return nil
	}
}

// ReceiveWithPrefetchCount configures the receiver to attempt to fetch as many messages as the prefetch amount
func ReceiveWithPrefetchCount(prefetch uint32) ReceiveOption {
	return func(receiver *receiver) error {
		receiver.prefetchCount = prefetch
		return nil
	}
}

// ReceiveWithEpoch configures the receiver to use an epoch -- see https://blogs.msdn.microsoft.com/gyan/2014/09/02/event-hubs-receiver-epoch/
func ReceiveWithEpoch(epoch int64) ReceiveOption {
	return func(receiver *receiver) error {
		receiver.epoch = &epoch
		return nil
	}
}

// newReceiver creates a new Service Bus message listener given an AMQP client and an entity path
func (h *hub) newReceiver(ctx context.Context, partitionID string, opts ...ReceiveOption) (*receiver, error) {
	receiver := &receiver{
		hub:           h,
		consumerGroup: DefaultConsumerGroup,
		prefetchCount: defaultPrefetchCount,
		partitionID:   partitionID,
	}

	for _, opt := range opts {
		if err := opt(receiver); err != nil {
			return nil, err
		}
	}

	receiver.debugLogf("creating a new receiver")
	err := receiver.newSessionAndLink(ctx)
	return receiver, err
}

// Close will close the AMQP session and link of the receiver
func (r *receiver) Close() error {
	if r.done != nil {
		r.done()
	}

	err := r.receiver.Close()
	if err != nil {
		_ = r.session.Close()
		return err
	}

	return r.session.Close()
}

// Recover will attempt to close the current session and link, then rebuild them
func (r *receiver) Recover(ctx context.Context) error {
	err := r.Close()
	if err != nil {
		return err
	}

	return r.newSessionAndLink(ctx)
}

// Listen start a listener for messages sent to the entity path
func (r *receiver) Listen(handler Handler) ListenerHandle {
	ctx, done := context.WithCancel(context.Background())
	r.done = done

	messages := make(chan *amqp.Message)
	go r.listenForMessages(ctx, messages)
	go r.handleMessages(ctx, messages, handler)

	return &listenerHandle{
		r:   r,
		ctx: ctx,
	}
}

func (r *receiver) handleMessages(ctx context.Context, messages chan *amqp.Message, handler Handler) {
	for {
		select {
		case <-ctx.Done():
			r.debugLogf("done handling messages")
			return
		case msg := <-messages:
			id := messageID(msg)
			r.debugLogf("message id: %v is being passed to handler", id)
			event := eventFromMsg(msg)
			err := handler(ctx, event)
			if err != nil {
				msg.Reject()
				r.debugLogf("message rejected: id: %v", id)
				continue
			}
			msg.Accept()
			r.debugLogf("message accepted: id: %v", id)
			r.storeLastReceivedOffset(event.GetCheckpoint())
		}
	}
}

func (r *receiver) listenForMessages(ctx context.Context, msgChan chan *amqp.Message) {
	for {
		msg, err := r.receiver.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.done()
			r.lastError = err
			return
		}

		id := messageID(msg)
		r.debugLogf("Message received: %s", id)
		select {
		case msgChan <- msg:
		case <-ctx.Done():
			return
		}
	}
}

// newSessionAndLink will replace the session and link on the receiver
func (r *receiver) newSessionAndLink(ctx context.Context) error {
	address := r.getAddress()
	err := r.hub.namespace.negotiateClaim(ctx, address)
	if err != nil {
		return err
	}

	connection, err := r.hub.namespace.connection()
	if err != nil {
		return err
	}

	amqpSession, err := connection.NewSession()
	if err != nil {
		return err
	}

	offsetExpression, err := r.getOffsetExpression()
	if err != nil {
		return err
	}

	r.session, err = newSession(amqpSession)
	if err != nil {
		return err
	}

	opts := []amqp.LinkOption{
		amqp.LinkSourceAddress(address),
		amqp.LinkCredit(r.prefetchCount),
		amqp.LinkSenderSettle(amqp.ModeUnsettled),
		amqp.LinkReceiverSettle(amqp.ModeSecond),
		amqp.LinkBatching(true),
		amqp.LinkSelectorFilter(offsetExpression),
	}

	if r.epoch != nil {
		opts = append(opts, amqp.LinkPropertyInt64(epochKey, *r.epoch))
	}

	amqpReceiver, err := amqpSession.NewReceiver(opts...)
	if err != nil {
		return err
	}

	r.receiver = amqpReceiver
	return nil
}

func (r *receiver) getLastReceivedOffset() (string, error) {
	checkpoint, err := r.offsetPersister().Read(r.namespaceName(), r.hubName(), r.consumerGroup, r.partitionID)
	return checkpoint.Offset, err
}

func (r *receiver) storeLastReceivedOffset(checkpoint persist.Checkpoint) error {
	return r.offsetPersister().Write(r.namespaceName(), r.hubName(), r.consumerGroup, r.partitionID, checkpoint)
}

func (r *receiver) getOffsetExpression() (string, error) {
	offset, err := r.getLastReceivedOffset()
	if err != nil {
		// assume err read is due to not having an offset -- probably want to change this as it's ambiguous
		return fmt.Sprintf(amqpAnnotationFormat, offsetAnnotationName, "=", persist.StartOfStream), nil
	}
	return fmt.Sprintf(amqpAnnotationFormat, offsetAnnotationName, "", offset), nil
}

func (r *receiver) getAddress() string {
	return fmt.Sprintf("%s/ConsumerGroups/%s/Partitions/%s", r.hubName(), r.consumerGroup, r.partitionID)
}

func (r *receiver) getIdentifier() string {
	if r.epoch != nil {
		return fmt.Sprintf("%s/ConsumerGroups/%s/Partitions/%s/epoch/%d", r.hubName(), r.consumerGroup, r.partitionID, *r.epoch)
	}
	return r.getAddress()
}

func (r *receiver) namespaceName() string {
	return r.hub.namespace.name
}

func (r *receiver) hubName() string {
	return r.hub.name
}

func (r *receiver) offsetPersister() persist.CheckpointPersister {
	return r.hub.offsetPersister
}

func messageID(msg *amqp.Message) interface{} {
	var id interface{} = "null"
	if msg.Properties != nil {
		id = msg.Properties.MessageID
	}
	return id
}

func (r *receiver) debugLogf(format string, args ...interface{}) {
	var msg string
	if len(args) > 0 {
		msg = fmt.Sprintf(format, args)
	} else {
		msg = format
	}

	log.Debugf(msg+" for entity identifier %q", r.getIdentifier())
}

func (lc *listenerHandle) Close() error {
	return lc.r.Close()
}

func (lc *listenerHandle) Done() <-chan struct{} {
	return lc.ctx.Done()
}

func (lc *listenerHandle) Err() error {
	if lc.r.lastError != nil {
		return lc.r.lastError
	}
	return lc.ctx.Err()
}
