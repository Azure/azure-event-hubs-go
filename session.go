package eventhub

import (
	"github.com/Azure/azure-event-hubs-go/internal/uuid"
	"pack.ag/amqp"
)

type (
	// session is a wrapper for the AMQP session with some added information to help with Service Bus messaging
	session struct {
		*amqp.Session
		SessionID string
	}
)

// newSession is a constructor for a Service Bus session which will pre-populate the SessionID with a new UUID
func newSession(amqpSession *amqp.Session) (*session, error) {
	sessionID, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	return &session{
		Session:   amqpSession,
		SessionID: sessionID.String(),
	}, nil
}

func (s *session) String() string {
	return s.SessionID
}
