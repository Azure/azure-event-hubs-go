package eventhub

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/Azure/go-amqp"
	"github.com/stretchr/testify/assert"
)

// conforms to amqpSender
type testAmqpSender struct {
	sendErrors []error
	sendCount  int
}

type recoveryCall struct {
	err     error
	recover bool
}

func (s *testAmqpSender) Send(ctx context.Context, msg *amqp.Message) error {
	var err error

	if len(s.sendErrors) > s.sendCount {
		err = s.sendErrors[s.sendCount]
	}

	s.sendCount++
	return err
}

func (s *testAmqpSender) Close(ctx context.Context) error {
	return nil
}

func TestSenderRetries(t *testing.T) {
	var recoverCalls []recoveryCall

	var sender *testAmqpSender

	getAmqpSender := func() amqpSender {
		return sender
	}

	recover := func(err error, recover bool) {
		recoverCalls = append(recoverCalls, recoveryCall{err, recover})
	}

	t.Run("SendOnFirstTry", func(t *testing.T) {
		recoverCalls = nil
		sender = &testAmqpSender{}

		err := sendMessage(context.TODO(), getAmqpSender, 3, nil, recover)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, sender.sendCount)
		assert.Empty(t, recoverCalls)
	})

	t.Run("SendExceedingRetries", func(*testing.T) {
		recoverCalls = nil
		sender = &testAmqpSender{
			sendErrors: []error{
				amqp.ErrLinkClosed,
				amqp.ErrSessionClosed,
				errors.New("We'll never attempt to use this one since we ran out of retries")},
		}

		actualErr := sendMessage(context.TODO(), getAmqpSender,
			1, // note we're only allowing 1 retry attempt - so we get the first send() and then 1 additional.
			nil, recover)

		assert.EqualValues(t, amqp.ErrSessionClosed, actualErr)
		assert.EqualValues(t, 2, sender.sendCount)
		assert.EqualValues(t, []recoveryCall{
			{
				err:     amqp.ErrLinkClosed,
				recover: true,
			},
			{
				err:     amqp.ErrSessionClosed,
				recover: true,
			},
		}, recoverCalls)

	})

	t.Run("SendWithUnrecoverableAndNonRetryableError", func(*testing.T) {
		recoverCalls = nil
		sender = &testAmqpSender{
			sendErrors: []error{
				errors.New("Anything not explicitly retryable kills all retries"),
				amqp.ErrConnClosed, // we'll never get here.
			},
		}

		actualErr := sendMessage(context.TODO(), getAmqpSender, 5, nil, recover)

		assert.EqualValues(t, errors.New("Anything not explicitly retryable kills all retries"), actualErr)
		assert.EqualValues(t, 1, sender.sendCount)
		assert.Empty(t, recoverCalls, "No recovery attempts should happen for non-recoverable errors")
	})

	t.Run("SendWithAmqpErrors", func(*testing.T) {
		recoverCalls = nil
		sender = &testAmqpSender{
			sendErrors: []error{&amqp.Error{
				// retry but doesn't attempt to recover the connection
				Condition: errorServerBusy,
			}, &amqp.Error{
				// retry but doesn't attempt to recover the connection
				Condition: errorTimeout,
			},
				&amqp.Error{
					// retry and will attempt to recover the connection
					Condition: amqp.ErrorNotImplemented,
				}},
		}

		err := sendMessage(context.TODO(), getAmqpSender, 6, nil, recover)
		assert.NoError(t, err)
		assert.EqualValues(t, 4, sender.sendCount)
		assert.EqualValues(t, []recoveryCall{
			{
				err: &amqp.Error{
					Condition: errorServerBusy,
				},
				recover: false,
			},
			{
				err: &amqp.Error{
					Condition: errorTimeout,
				},
				recover: false,
			},
			{
				err: &amqp.Error{
					Condition: amqp.ErrorNotImplemented,
				},
				recover: true,
			},
		}, recoverCalls)
	})

	t.Run("SendWithDetachOrNetErrors", func(*testing.T) {
		recoverCalls = nil
		sender = &testAmqpSender{
			sendErrors: []error{
				&amqp.DetachError{},
				&net.DNSError{},
			},
		}

		err := sendMessage(context.TODO(), getAmqpSender, 6, nil, recover)
		assert.NoError(t, err)
		assert.EqualValues(t, 3, sender.sendCount)
		assert.EqualValues(t, []recoveryCall{
			{
				err:     &amqp.DetachError{},
				recover: true,
			},
			{
				err:     &net.DNSError{},
				recover: true,
			},
		}, recoverCalls)
	})

	t.Run("SendWithRecoverableCloseError", func(*testing.T) {
		recoverCalls = nil
		sender = &testAmqpSender{
			sendErrors: []error{
				amqp.ErrConnClosed,
				amqp.ErrLinkClosed,
				amqp.ErrSessionClosed,
			},
		}

		err := sendMessage(context.TODO(), getAmqpSender, 6, nil, recover)
		assert.NoError(t, err)
		assert.EqualValues(t, 4, sender.sendCount)
		assert.EqualValues(t, []recoveryCall{
			{
				err:     amqp.ErrConnClosed,
				recover: true,
			},
			{
				err:     amqp.ErrLinkClosed,
				recover: true,
			},
			{
				err:     amqp.ErrSessionClosed,
				recover: true,
			},
		}, recoverCalls)
	})

}
