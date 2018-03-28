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
	"github.com/satori/uuid"
	log "github.com/sirupsen/logrus"
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
	SendOption func(message *amqp.Message) error
)

// newSender creates a new Service Bus message sender given an AMQP client and entity path
func (h *Hub) newSender(ctx context.Context) (*sender, error) {
	s := &sender{
		hub:         h,
		partitionID: h.senderPartitionID,
	}
	log.Debugf("creating a new sender for entity path %s", s.getAddress())
	err := s.newSessionAndLink(ctx)
	return s, err
}

// Recover will attempt to close the current session and link, then rebuild them
func (s *sender) Recover(ctx context.Context) error {
	_ = s.Close() // we expect the sender is in an error state
	return s.newSessionAndLink(ctx)
}

// Close will close the AMQP connection, session and link of the sender
func (s *sender) Close() error {
	err := s.connection.Close()
	if err != nil {
		_ = s.session.Close()
		_ = s.sender.Close()
		return err
	}
	err = s.session.Close()
	if err != nil {
		_ = s.sender.Close()
		return err
	}
	return s.sender.Close()
}

// Send will send a message to the entity path with options
//
// This will retry sending the message if the server responds with a busy error.
func (s *sender) Send(ctx context.Context, msg *amqp.Message, opts ...SendOption) error {
	s.prepareMessage(msg)

	for _, opt := range opts {
		err := opt(msg)
		if err != nil {
			return err
		}
	}

	if msg.Properties.MessageID == nil {
		msg.Properties.MessageID = uuid.NewV4().String()
	}

	times := 3
	delay := 10 * time.Second
	durationOfSend := 3 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		times = int(time.Until(deadline) / (delay + durationOfSend))
		times = max(times, 1) // give at least one chance at sending
	}
	_, err := common.Retry(times, delay, func() (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			innerCtx, cancel := context.WithTimeout(ctx, durationOfSend)
			defer cancel()
			err := s.sender.Send(innerCtx, msg)

			if err != nil {
				log.Warnln("recovering...", err)
				recoverErr := s.Recover(ctx)
				if recoverErr != nil {
					log.Errorln(recoverErr)
				}
			}

			if amqpErr, ok := err.(*amqp.Error); ok {
				if amqpErr.Condition == "com.microsoft:server-busy" {
					log.Warnln("retrying... ", amqpErr)
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

func (s *sender) prepareMessage(msg *amqp.Message) {
	if msg.Properties == nil {
		msg.Properties = &amqp.MessageProperties{}
	}

	if msg.Annotations == nil {
		msg.Annotations = make(map[interface{}]interface{})
	}
}

// newSessionAndLink will replace the existing session and link
func (s *sender) newSessionAndLink(ctx context.Context) error {
	connection, err := s.hub.namespace.newConnection()
	if err != nil {
		return err
	}
	s.connection = connection

	err = s.hub.namespace.negotiateClaim(ctx, connection, s.getAddress())
	if err != nil {
		return err
	}

	amqpSession, err := connection.NewSession()
	if err != nil {
		return err
	}

	amqpSender, err := amqpSession.NewSender(amqp.LinkTargetAddress(s.getAddress()))
	if err != nil {
		return err
	}

	s.session, err = newSession(amqpSession)
	if err != nil {
		return err
	}

	s.sender = amqpSender
	return nil
}

// SendWithMessageID configures the message with a message ID
func SendWithMessageID(messageID string) SendOption {
	return func(msg *amqp.Message) error {
		msg.Properties.MessageID = messageID
		return nil
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
