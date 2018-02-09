package eventhub

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"pack.ag/amqp"
	"sync/atomic"
	"time"
)

const (
	// DefaultConsumerGroup is the default name for a event stream consumer group
	DefaultConsumerGroup = "$Default"

	// StartOfStream is a constant defined to represent the start of a partition stream in EventHub.
	StartOfStream = -1

	// EndOfStream is a constant defined to represent the current end of a partition stream in EventHub.
	// This can be used as an offset argument in receiver creation to start receiving from the latest
	// event, instead of a specific offset or point in time.
	EndOfStream = "@latest"

	amqpAnnotationFormat = "amqp.annotation.%s >%s '%s'"
	offsetAnnotationName = "x-opt-offset"
	defaultPrefetchCount = 100
)

// receiver provides session and link handling for a receiving entity path
type (
	receiver struct {
		hub                *hub
		session            *session
		receiver           *amqp.Receiver
		consumerGroup      string
		streamPosition     int64
		partitionID        string
		prefetchCount      uint32
		done               chan struct{}
		lastReceivedOffset atomic.Value
	}

	// ReceiveOption provides a structure for configuring receivers
	ReceiveOption func(receiver *receiver) error
)

// ReceiveWithConsumerGroup configures the receiver to listen to a specific consumer group
func ReceiveWithConsumerGroup(consumerGroup string) ReceiveOption {
	return func(receiver *receiver) error {
		receiver.consumerGroup = consumerGroup
		return nil
	}
}

// ReceiveWithStreamPosition configures the receiver to start at a given position in the event stream
func ReceiveWithStreamPosition(position int64) ReceiveOption {
	return func(receiver *receiver) error {
		receiver.streamPosition = position
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

// newReceiver creates a new Service Bus message listener given an AMQP client and an entity path
func (h *hub) newReceiver(partitionID string, opts ...ReceiveOption) (*receiver, error) {
	receiver := &receiver{
		hub:            h,
		consumerGroup:  DefaultConsumerGroup,
		streamPosition: StartOfStream,
		prefetchCount:  defaultPrefetchCount,
		partitionID:    partitionID,
		done:           make(chan struct{}),
	}

	for _, opt := range opts {
		if err := opt(receiver); err != nil {
			return nil, err
		}
	}

	log.Debugf("creating a new receiver for entity path %s", receiver.getAddress())
	err := receiver.newSessionAndLink()
	if err != nil {
		return nil, err
	}
	return receiver, nil
}

// Close will close the AMQP session and link of the receiver
func (r *receiver) Close() error {
	close(r.done)

	err := r.receiver.Close()
	if err != nil {
		return err
	}

	err = r.session.Close()
	if err != nil {
		return err
	}

	return nil
}

// Recover will attempt to close the current session and link, then rebuild them
func (r *receiver) Recover() error {
	err := r.Close()
	if err != nil {
		return err
	}

	err = r.newSessionAndLink()
	if err != nil {
		return err
	}

	return nil
}

// Listen start a listener for messages sent to the entity path
func (r *receiver) Listen(handler Handler) {
	messages := make(chan *amqp.Message)
	go r.listenForMessages(messages)
	go r.handleMessages(messages, handler)
}

func (r *receiver) handleMessages(messages chan *amqp.Message, handler Handler) {
	for {
		select {
		case <-r.done:
			log.Debug("done handling messages")
			close(messages)
			return
		case msg := <-messages:
			ctx := context.Background()
			id := interface{}("null")
			if msg.Properties != nil {
				id = msg.Properties.MessageID
			}
			log.Debugf("message id: %v is being passed to handler", id)

			err := handler(ctx, msg)
			if err != nil {
				msg.Reject()
				log.Debugf("message rejected: id: %v", id)
			} else {
				// Accept message
				msg.Accept()
				log.Debugf("message accepted: id: %v", id)
			}
		}
	}
}

func (r *receiver) listenForMessages(msgChan chan *amqp.Message) {
	for {
		select {
		case <-r.done:
			log.Debug("done listenting for messages")
			return
		default:
			//log.Debug("attempting to receive messages")
			waitCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			msg, err := r.receiver.Receive(waitCtx)
			cancel()

			// TODO: handle receive errors better. It's not sufficient to check only for timeout
			if err, ok := err.(net.Error); ok && err.Timeout() {
				log.Debug("attempting to receive messages timed out")
				continue
			} else if err != nil {
				log.Fatalln(err)
				time.Sleep(10 * time.Second)
			}
			if msg != nil {
				id := interface{}("null")
				if msg.Properties != nil {
					id = msg.Properties.MessageID
				}
				log.Debugf("message received: %v", id)

				if msg.Annotations == nil {
					// this case should not happen and will cause replay of the event log
					log.Warnln("message id: %v does not have annotations and will not have an offset.", id)
				} else {
					if offset, ok := msg.Annotations[offsetAnnotationName]; ok {
						log.Debugf("message id: %v has offset of %s", id, offset)
						r.storeLastReceivedOffset(offset.(string))
					} else {
						// this case should not happen and will cause replay of the event log
						log.Warnln("message id: %v has annotations, but doesn't contain an offset.", id)
					}
				}
				msgChan <- msg
			}
		}
	}
}

// newSessionAndLink will replace the session and link on the receiver
func (r *receiver) newSessionAndLink() error {
	address := r.getAddress()
	if r.hub.namespace.claimsBasedSecurityEnabled() {
		err := r.hub.namespace.negotiateClaim(address)
		if err != nil {
			return err
		}
	}

	connection, err := r.hub.namespace.connection()
	if err != nil {
		return err
	}

	amqpSession, err := connection.NewSession()
	if err != nil {
		return err
	}

	r.session = newSession(amqpSession)
	opts := []amqp.LinkOption{
		amqp.LinkSourceAddress(address),
		amqp.LinkCredit(r.prefetchCount),
		amqp.LinkSenderSettle(amqp.ModeUnsettled),
		amqp.LinkReceiverSettle(amqp.ModeSecond),
		amqp.LinkBatching(true),
	}

	amqpReceiver, err := amqpSession.NewReceiver(opts...)
	if err != nil {
		return err
	}

	r.receiver = amqpReceiver
	return nil
}

func (r *receiver) getLastReceivedOffset() string {
	return r.lastReceivedOffset.Load().(string)
}

func (r *receiver) storeLastReceivedOffset(offset string) {
	r.lastReceivedOffset.Store(offset)
}

func (r *receiver) getOffsetExpression() string {
	if r.getLastReceivedOffset() != "" {
		return fmt.Sprintf(amqpAnnotationFormat, offsetAnnotationName, "", r.getLastReceivedOffset())
	}

	return fmt.Sprintf(amqpAnnotationFormat, offsetAnnotationName, "=", string(r.streamPosition))
}

func (r *receiver) getAddress() string {
	return fmt.Sprintf("%s/ConsumerGroups/%s/Partitions/%s", r.hub.name, r.consumerGroup, r.partitionID)
}
