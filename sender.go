package eventhub

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"pack.ag/amqp"
)

// sender provides session and link handling for an sending entity path
type (
	sender struct {
		hub         *hub
		session     *session
		sender      *amqp.Sender
		partitionID *string
		Name        string
	}

	// SendOption provides a way to customize a message on sending
	SendOption func(message *amqp.Message) error
)

// newSender creates a new Service Bus message sender given an AMQP client and entity path
func (h *hub) newSender() (*sender, error) {
	s := &sender{
		hub:         h,
		partitionID: h.senderPartitionID,
	}

	log.Debugf("creating a new sender for entity path %s", s.getAddress())
	err := s.newSessionAndLink()
	if err != nil {
		return nil, err
	}

	return s, nil
}

// Recover will attempt to close the current session and link, then rebuild them
func (s *sender) Recover() error {
	err := s.Close()
	if err != nil {
		return err
	}

	err = s.newSessionAndLink()
	if err != nil {
		return err
	}

	return nil
}

// Close will close the AMQP session and link of the sender
func (s *sender) Close() error {
	err := s.sender.Close()
	if err != nil {
		return err
	}

	err = s.session.Close()
	if err != nil {
		return err
	}
	return nil
}

// Send will send a message to the entity path with options
func (s *sender) Send(ctx context.Context, msg *amqp.Message, opts ...SendOption) error {
	// TODO: Add in recovery logic in case the link / session has gone down
	s.prepareMessage(msg)

	for _, opt := range opts {
		err := opt(msg)
		if err != nil {
			return err
		}
	}

	if s.partitionID != nil {
		msg.Annotations["x-opt-partition-key"] = s.partitionID
	}

	err := s.sender.Send(ctx, msg)
	if err != nil {
		return err
	}
	return nil
}

//func (s *sender) SendBatch(ctx context.Context, messages []*amqp.Message) error {
//
//}

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
func (s *sender) newSessionAndLink() error {
	if s.hub.namespace.claimsBasedSecurityEnabled() {
		err := s.hub.namespace.negotiateClaim(s.getAddress())
		if err != nil {
			return err
		}
	}

	connection, err := s.hub.namespace.connection()
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

	s.session = newSession(amqpSession)
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

// SendWithSession configures the message to send with a specific session and sequence. By default, a sender has a
// default session (uuid.NewV4()) and sequence generator.
func SendWithSession(sessionID string, sequence uint32) SendOption {
	return func(msg *amqp.Message) error {
		msg.Properties.GroupID = sessionID
		msg.Properties.GroupSequence = sequence
		return nil
	}
}
