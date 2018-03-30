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
	"github.com/Azure/azure-amqp-common-go/uuid"
	"github.com/opentracing/opentracing-go"
	"pack.ag/amqp"
)

// sender provides session and link handling for an sending entity path
type (
	sender struct {
		hub         *Hub
		connection  *amqp.Client
		session     *session
		sender      *amqp.Sender
		partitionID *string
		Name        string
	}

	// SendOption provides a way to customize a message on sending
	SendOption func(event *Event) error

	eventer interface {
		Set(key, value string)
		toMsg() *amqp.Message
	}
)

// newSender creates a new Service Bus message sender given an AMQP client and entity path
func (h *Hub) newSender(ctx context.Context) (*sender, error) {
	span, ctx := h.startSpanFromContext(ctx, "eventhub.sender.newSender")
	defer span.Finish()

	s := &sender{
		hub:         h,
		partitionID: h.senderPartitionID,
	}
	log.For(ctx).Debug(fmt.Sprintf("creating a new sender for entity path %s", s.getAddress()))
	err := s.newSessionAndLink(ctx)
	return s, err
}

// Recover will attempt to close the current session and link, then rebuild them
func (s *sender) Recover(ctx context.Context) error {
	span, ctx := s.startProducerSpanFromContext(ctx, "eventhub.sender.Recover")
	defer span.Finish()

	_ = s.Close(ctx) // we expect the sender is in an error state
	return s.newSessionAndLink(ctx)
}

// Close will close the AMQP connection, session and link of the sender
func (s *sender) Close(ctx context.Context) error {
	err := s.connection.Close()
	if err != nil {
		_ = s.session.Close(ctx)
		_ = s.sender.Close(ctx)
		return err
	}
	err = s.session.Close(ctx)
	if err != nil {
		_ = s.sender.Close(ctx)
		return err
	}
	return s.sender.Close(ctx)
}

// Send will send a message to the entity path with options
//
// This will retry sending the message if the server responds with a busy error.
func (s *sender) Send(ctx context.Context, event *Event, opts ...SendOption) error {
	span, ctx := s.startProducerSpanFromContext(ctx, "eventhub.sender.Send")
	defer span.Finish()

	for _, opt := range opts {
		err := opt(event)
		if err != nil {
			return err
		}
	}

	if event.ID == "" {
		id, err := uuid.NewV4()
		if err != nil {
			return err
		}
		event.ID = id.String()
	}

	return s.trySend(ctx, event)
}

func (s *sender) trySend(ctx context.Context, evt eventer) error {
	sp, ctx := s.startProducerSpanFromContext(ctx, "eventhub.sender.trySend")
	defer sp.Finish()

	times := 3
	delay := 10 * time.Second
	durationOfSend := 3 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		times = int(time.Until(deadline) / (delay + durationOfSend))
		times = max(times, 1) // give at least one chance at sending
	}
	_, err := common.Retry(times, delay, func() (interface{}, error) {
		sp, ctx := s.startProducerSpanFromContext(ctx, "eventhub.sender.trySend.transmit")
		defer sp.Finish()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			innerCtx, cancel := context.WithTimeout(ctx, durationOfSend)
			defer cancel()

			err := opentracing.GlobalTracer().Inject(sp.Context(), opentracing.TextMap, evt)
			if err != nil {
				log.For(ctx).Error(err)
				return nil, err
			}

			msg := evt.toMsg()
			sp.SetTag("eventhub.message-id", msg.Properties.MessageID)
			err = s.sender.Send(innerCtx, msg)
			if err != nil {
				recoverErr := s.Recover(ctx)
				if recoverErr != nil {
					log.For(ctx).Error(recoverErr)
				}
			}

			if amqpErr, ok := err.(*amqp.Error); ok {
				if amqpErr.Condition == "com.microsoft:server-busy" {
					return nil, common.Retryable(amqpErr.Condition)
				}
			}

			return nil, err
		}
	})
	return err
}

func (s *sender) String() string {
	return s.Name
}

func (s *sender) getAddress() string {
	if s.partitionID != nil {
		return fmt.Sprintf("%s/Partitions/%s", s.hub.name, *s.partitionID)
	}
	return s.hub.name
}

func (s *sender) getFullIdentifier() string {
	return s.hub.namespace.getEntityAudience(s.getAddress())
}

// newSessionAndLink will replace the existing session and link
func (s *sender) newSessionAndLink(ctx context.Context) error {
	span, ctx := s.startProducerSpanFromContext(ctx, "eventhub.sender.newSessionAndLink")
	defer span.Finish()

	connection, err := s.hub.namespace.newConnection()
	if err != nil {
		log.For(ctx).Error(err)
		return err
	}
	s.connection = connection

	err = s.hub.namespace.negotiateClaim(ctx, connection, s.getAddress())
	if err != nil {
		log.For(ctx).Error(err)
		return err
	}

	amqpSession, err := connection.NewSession()
	if err != nil {
		log.For(ctx).Error(err)
		return err
	}

	amqpSender, err := amqpSession.NewSender(amqp.LinkTargetAddress(s.getAddress()))
	if err != nil {
		log.For(ctx).Error(err)
		return err
	}

	s.session, err = newSession(amqpSession)
	if err != nil {
		log.For(ctx).Error(err)
		return err
	}

	s.sender = amqpSender
	return nil
}

// SendWithMessageID configures the message with a message ID
func SendWithMessageID(messageID string) SendOption {
	return func(event *Event) error {
		event.ID = messageID
		return nil
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
