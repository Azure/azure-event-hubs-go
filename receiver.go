package eventhub

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
	"time"

	"github.com/Azure/azure-amqp-common-go"
	"github.com/Azure/azure-amqp-common-go/log"
	"github.com/Azure/azure-amqp-common-go/persist"
	"github.com/Azure/azure-event-hubs-go/mgmt"
	"github.com/opentracing/opentracing-go"
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
		hub           *Hub
		connection    *amqp.Client
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

	// ListenerHandle provides the ability to close or listen to the close of a Receiver
	ListenerHandle struct {
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
func (h *Hub) newReceiver(ctx context.Context, partitionID string, opts ...ReceiveOption) (*receiver, error) {
	span, ctx := h.startSpanFromContext(ctx, "eventhub.Hub.newReceiver")
	defer span.Finish()

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

	log.For(ctx).Debug("creating a new receiver")
	err := receiver.newSessionAndLink(ctx)
	return receiver, err
}

// Close will close the AMQP session and link of the receiver
func (r *receiver) Close(ctx context.Context) error {
	if r.done != nil {
		r.done()
	}

	err := r.receiver.Close(ctx)
	if err != nil {
		_ = r.session.Close(ctx)
		_ = r.connection.Close()
		return err
	}

	err = r.session.Close(ctx)
	if err != nil {
		_ = r.connection.Close()
		return err
	}

	return r.connection.Close()
}

// Recover will attempt to close the current session and link, then rebuild them
func (r *receiver) Recover(ctx context.Context) error {
	span, ctx := r.startConsumerSpanFromContext(ctx, "eventhub.receiver.Recover")
	defer span.Finish()

	_ = r.Close(ctx) // we expect the receiver is in an error state
	return r.newSessionAndLink(ctx)
}

// Listen start a listener for messages sent to the entity path
func (r *receiver) Listen(handler Handler) *ListenerHandle {
	ctx, done := context.WithCancel(context.Background())
	r.done = done

	span, ctx := r.startConsumerSpanFromContext(ctx, "eventhub.receiver.Listen")
	defer span.Finish()

	messages := make(chan *amqp.Message)
	go r.listenForMessages(ctx, messages)
	go r.handleMessages(ctx, messages, handler)

	return &ListenerHandle{
		r:   r,
		ctx: ctx,
	}
}

func (r *receiver) handleMessages(ctx context.Context, messages chan *amqp.Message, handler Handler) {
	span, ctx := r.startConsumerSpanFromContext(ctx, "eventhub.receiver.handleMessages")
	defer span.Finish()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-messages:
			r.handleMessage(ctx, msg, handler)
		}
	}
}

func (r *receiver) handleMessage(ctx context.Context, msg *amqp.Message, handler Handler) {
	event := eventFromMsg(msg)
	var span opentracing.Span
	wireContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, event)
	if err == nil {
		span, ctx = r.startConsumerSpanFromWire(ctx, "eventhub.receiver.handleMessage", wireContext)
	} else {
		span, ctx = r.startConsumerSpanFromContext(ctx, "eventhub.receiver.handleMessage")
	}
	defer span.Finish()

	id := messageID(msg)
	span.SetTag("eventhub.message-id", id)

	err = handler(ctx, event)
	if err != nil {
		msg.Reject()
		log.For(ctx).Error(fmt.Errorf("message rejected: id: %v", id))
		return
	}
	msg.Accept()
	r.storeLastReceivedOffset(event.GetCheckpoint())
}

func (r *receiver) listenForMessages(ctx context.Context, msgChan chan *amqp.Message) {
	span, ctx := r.startConsumerSpanFromContextFollowing(ctx, "eventhub.receiver.listenForMessages")
	defer span.Finish()

	for {
		msg, err := r.listenForMessage(ctx)
		if err != nil && ctx.Err() == nil {
			_, retryErr := common.Retry(5, 10*time.Second, func() (interface{}, error) {
				sp, ctx := r.startConsumerSpanFromContext(ctx, "eventhub.receiver.listenForMessages.tryRecover")
				defer sp.Finish()

				err := r.Recover(ctx)
				if err != nil {
					log.For(ctx).Error(err)
					return nil, common.Retryable(err.Error())
				}
				return nil, nil
			})

			if retryErr != nil {
				r.lastError = retryErr
				r.Close(ctx)
				return
			}
			continue
		}
		select {
		case msgChan <- msg:
		case <-ctx.Done():
			return
		}
	}
}

func (r *receiver) listenForMessage(ctx context.Context) (*amqp.Message, error) {
	span, ctx := r.startConsumerSpanFromContext(ctx, "eventhub.receiver.listenForMessage")
	defer span.Finish()

	msg, err := r.receiver.Receive(ctx)
	if err != nil {
		if ctx.Err() != nil {
			log.For(ctx).Error(ctx.Err())
			return nil, ctx.Err()
		}
		r.done()
		r.lastError = err
		log.For(ctx).Error(err)
		return nil, err
	}

	id := messageID(msg)
	span.SetTag("eventhub.message-id", id)
	return msg, nil
}

// newSessionAndLink will replace the session and link on the receiver
func (r *receiver) newSessionAndLink(ctx context.Context) error {
	span, ctx := r.startConsumerSpanFromContext(ctx, "eventhub.receiver.newSessionAndLink")
	defer span.Finish()

	connection, err := r.hub.namespace.newConnection()
	if err != nil {
		return err
	}
	r.connection = connection

	address := r.getAddress()
	err = r.hub.namespace.negotiateClaim(ctx, connection, address)
	if err != nil {
		log.For(ctx).Error(err)
		return err
	}

	amqpSession, err := connection.NewSession()
	if err != nil {
		log.For(ctx).Error(err)
		return err
	}

	offsetExpression, err := r.getOffsetExpression()
	if err != nil {
		log.For(ctx).Error(err)
		return err
	}

	r.session, err = newSession(amqpSession)
	if err != nil {
		log.For(ctx).Error(err)
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
		log.For(ctx).Error(err)
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

func (r *receiver) getFullIdentifier() string {
	return r.hub.namespace.getEntityAudience(r.getIdentifier())
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

// Close will close the listener
func (lc *ListenerHandle) Close(ctx context.Context) error {
	return lc.r.Close(ctx)
}

// Done will close the channel when the listener has stopped
func (lc *ListenerHandle) Done() <-chan struct{} {
	return lc.ctx.Done()
}

// Err will return the last error encountered
func (lc *ListenerHandle) Err() error {
	if lc.r.lastError != nil {
		return lc.r.lastError
	}
	return lc.ctx.Err()
}
