package rpc

import (
	"context"
	"fmt"
	"github.com/Azure/azure-event-hubs-go/common"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"pack.ag/amqp"
	"strings"
	"sync"
	"time"
)

const (
	replyPostfix   = "-reply-to-"
	statusCodeKey  = "status-code"
	descriptionKey = "status-description"
)

type (
	// Link is the bidirectional communication structure used for CBS negotiation
	Link struct {
		session       *amqp.Session
		receiver      *amqp.Receiver
		sender        *amqp.Sender
		clientAddress string
		rpcMu         sync.Mutex
		id            string
	}

	// Response is the simplified response structure from an RPC like call
	Response struct {
		Code        int
		Description string
		Message     *amqp.Message
	}
)

// NewLink will build a new request response link
func NewLink(conn *amqp.Client, address string) (*Link, error) {
	authSession, err := conn.NewSession()
	if err != nil {
		return nil, err
	}

	authSender, err := authSession.NewSender(
		amqp.LinkTargetAddress(address),
	)
	if err != nil {
		return nil, err
	}

	id := uuid.NewV4().String()
	clientAddress := strings.Replace("$", "", address, -1) + replyPostfix + id
	authReceiver, err := authSession.NewReceiver(
		amqp.LinkSourceAddress(address),
		amqp.LinkTargetAddress(clientAddress))
	if err != nil {
		return nil, err
	}

	return &Link{
		sender:        authSender,
		receiver:      authReceiver,
		session:       authSession,
		clientAddress: clientAddress,
		id:            id,
	}, nil
}

// RetryableRPC attempts to retry a request a number of times with delay
func (l *Link) RetryableRPC(ctx context.Context, times int, delay time.Duration, msg *amqp.Message) (*Response, error) {
	res, err := common.Retry(times, delay, func() (interface{}, error) {
		res, err := l.RPC(ctx, msg)
		if err != nil {
			log.Debugf("error in RPC via link %s: %v", l.id, err)
			return nil, err
		}

		if res.ServerError() {
			errMessage := fmt.Sprintf("server error link %s: status code %d and description: %s", l.id, res.Code, res.Description)
			log.Debugln(errMessage)
			return nil, &common.Retryable{Message: errMessage}
		}

		if res.ClientError() {
			errMessage := fmt.Sprintf("client error link %s: status code %d and description: %s", l.id, res.Code, res.Description)
			log.Debugln(errMessage)
			return nil, errors.New(errMessage)
		}

		if res.Success() {
			log.Debugf("successful rpc on link %s: status code %d and description: %s", l.id, res.Code, res.Description)
			return res, nil
		}

		errMessage := fmt.Sprintf("unhandled error link %s: status code %d and description: %s", l.id, res.Code, res.Description)
		log.Debugln(errMessage)
		return nil, &common.Retryable{Message: errMessage}
	})
	if err != nil {
		return nil, err
	}
	return res.(*Response), err
}

// RPC sends a request and waits on a response for that request
func (l *Link) RPC(ctx context.Context, msg *amqp.Message) (*Response, error) {
	l.rpcMu.Lock()
	defer l.rpcMu.Unlock()

	if msg.Properties == nil {
		msg.Properties = &amqp.MessageProperties{}
	}
	msg.Properties.ReplyTo = l.clientAddress

	err := l.sender.Send(ctx, msg)
	if err != nil {
		return nil, err
	}

	res, err := l.receiver.Receive(ctx)
	if err != nil {
		return nil, err
	}

	response := &Response{
		Message: res,
	}

	if statusCode, ok := res.ApplicationProperties[statusCodeKey].(int32); ok {
		response.Code = int(statusCode)
	} else {
		return nil, errors.New("status codes was not found on rpc message")
	}

	if description, ok := res.ApplicationProperties[descriptionKey].(string); ok {
		response.Description = description
	} else {
		return nil, errors.New("description was not found on rpc message")
	}

	return response, err
}

// Close the link sender, receiver and session
func (l *Link) Close() {
	if l.sender != nil {
		l.sender.Close()
	}

	if l.receiver != nil {
		l.receiver.Close()
	}

	if l.session != nil {
		l.session.Close()
	}
}

// Success return true if the status code is between 200 and 300
func (r *Response) Success() bool {
	return r.Code >= 200 && r.Code < 300
}

// ServerError is true when status code is 500 or greater
func (r *Response) ServerError() bool {
	return r.Code >= 500
}

// ClientError is true when status code is in the 400s
func (r *Response) ClientError() bool {
	return r.Code >= 400 && r.Code < 500
}
