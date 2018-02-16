package cbs

import (
	"context"
	"fmt"
	"github.com/Azure/azure-event-hubs-go/common"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"pack.ag/amqp"
	"sync"
	"time"
)

const (
	// CbsTokenTypeJwt is the type of token to be used for JWT tokens. For example Azure Active Directory tokens.
	CbsTokenTypeJwt = "jwt"
	// CbsTokenTypeSas is the type of token to be used for SAS tokens.
	CbsTokenTypeSas = "servicebus.windows.net:sastoken"

	cbsAddress           = "$cbs"
	cbsReplyToPrefix     = "cbs-tmp-"
	cbsOperationKey      = "operation"
	cbsOperationPutToken = "put-token"
	cbsTokenTypeKey      = "type"
	cbsAudienceKey       = "name"
	cbsExpirationKey     = "expiration"
	cbsStatusCodeKey     = "status-code"
	cbsDescriptionKey    = "status-description"
)

type (
	// Link is the bidirectional communication structure used for CBS negotiation
	Link struct {
		session       *amqp.Session
		receiver      *amqp.Receiver
		sender        *amqp.Sender
		clientAddress string
		negotiateMu   sync.Mutex
	}

	// Token contains all of the information to negotiate CBS
	Token struct {
		tokenType string
		token     string
		expiry    string
	}

	// TokenProvider abstracts the fetching of CBS tokens
	TokenProvider interface {
		GetToken(uri string) (*Token, error)
	}
)

// NewToken constructs a new CBS token
func NewToken(tokenType, token, expiry string) *Token {
	return &Token{
		tokenType: tokenType,
		token:     token,
		expiry:    expiry,
	}
}

// NewLink will build a new CBS link
func NewLink(conn *amqp.Client) (*Link, error) {
	authSession, err := conn.NewSession()
	if err != nil {
		return nil, err
	}

	authSender, err := authSession.NewSender(amqp.LinkTargetAddress(cbsAddress))
	if err != nil {
		return nil, err
	}

	cbsClientAddress := cbsReplyToPrefix + uuid.NewV4().String()
	authReceiver, err := authSession.NewReceiver(
		amqp.LinkSourceAddress(cbsAddress),
		amqp.LinkTargetAddress(cbsClientAddress))
	if err != nil {
		return nil, err
	}

	return &Link{
		sender:        authSender,
		receiver:      authReceiver,
		session:       authSession,
		clientAddress: cbsClientAddress,
	}, nil
}

// NegotiateClaim attempts to put a token to the $cbs management endpoint to negotiate auth for the given audience
func NegotiateClaim(audience string, link *Link, provider TokenProvider) error {
	link.negotiateMu.Lock()
	defer link.negotiateMu.Unlock()

	token, err := provider.GetToken(audience)
	if err != nil {
		return err
	}

	log.Debugf("sending to: %s, expiring on: %q, via: %s", audience, token.expiry, link.clientAddress)
	msg := &amqp.Message{
		Value: token.token,
		Properties: &amqp.MessageProperties{
			ReplyTo: link.clientAddress,
		},
		ApplicationProperties: map[string]interface{}{
			cbsOperationKey:  cbsOperationPutToken,
			cbsTokenTypeKey:  token.tokenType,
			cbsAudienceKey:   audience,
			cbsExpirationKey: token.expiry,
		},
	}

	_, err = common.Retry(3, 1*time.Second, func() (interface{}, error) {
		log.Debugf("Attempting to negotiate cbs for audience %s", audience)
		err := link.send(context.Background(), msg)
		if err != nil {
			return nil, err
		}

		res, err := link.receive(context.Background())
		if err != nil {
			return nil, err
		}

		if statusCode, ok := res.ApplicationProperties[cbsStatusCodeKey].(int32); ok {
			description := res.ApplicationProperties[cbsDescriptionKey].(string)
			if statusCode >= 200 && statusCode < 300 {
				log.Debugf("Successfully negotiated cbs for audience %s", audience)
				return res, nil
			} else if statusCode >= 500 {
				log.Debugf("Re-negotiating cbs for audience %s. Received status code: %d and error: %s", audience, statusCode, description)
				return nil, &common.Retryable{Message: "cbs error: " + description}
			} else {
				log.Debugf("Failed negotiating cbs for audience %s with error %d and message %s", audience, statusCode, description)
				return nil, fmt.Errorf("cbs error: failed with code %d and message: %s", statusCode, description)
			}
		}

		return nil, &common.Retryable{Message: "cbs error: didn't understand the replied message status code"}
	})

	return err
}

func (cl *Link) forceClose() {
	if cl.sender != nil {
		cl.sender.Close()
	}

	if cl.receiver != nil {
		cl.receiver.Close()
	}

	if cl.session != nil {
		cl.session.Close()
	}
}

func (cl *Link) send(ctx context.Context, msg *amqp.Message) error {
	return cl.sender.Send(ctx, msg)
}

func (cl *Link) receive(ctx context.Context) (*amqp.Message, error) {
	return cl.receiver.Receive(ctx)
}
