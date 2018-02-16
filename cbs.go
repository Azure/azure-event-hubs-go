package eventhub

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"pack.ag/amqp"
	"sync"
	"time"
)

const (
	cbsAddress           = "$cbs"
	cbsReplyToPrefix     = "cbs-tmp-"
	cbsOperationKey      = "operation"
	cbsOperationPutToken = "put-token"
	cbsTokenTypeKey      = "type"
	cbsTokenTypeJwt      = "jwt"
	cbsAudienceKey       = "name"
	cbsExpirationKey     = "expiration"
	cbsStatusCodeKey     = "status-code"
	cbsDescriptionKey    = "status-description"
)

type (
	cbsLink struct {
		session       *amqp.Session
		receiver      *amqp.Receiver
		sender        *amqp.Sender
		clientAddress string
		negotiateMu   sync.Mutex
	}
)

func (ns *Namespace) newCbsLink() (*cbsLink, error) {
	conn, err := ns.connection()
	if err != nil {
		return nil, err
	}
	authSession, err := conn.NewSession()
	if err != nil {
		return nil, err
	}

	authSender, err := authSession.NewSender(amqp.LinkTargetAddress(cbsAddress))
	if err != nil {
		return nil, err
	}

	cbsClientAddress := cbsReplyToPrefix + ns.name
	authReceiver, err := authSession.NewReceiver(
		amqp.LinkSourceAddress(cbsAddress),
		amqp.LinkTargetAddress(cbsClientAddress))
	if err != nil {
		return nil, err
	}

	return &cbsLink{
		sender:        authSender,
		receiver:      authReceiver,
		session:       authSession,
		clientAddress: cbsClientAddress,
	}, nil
}

func (ns *Namespace) ensureCbsLink() error {
	ns.cbsMu.Lock()
	defer ns.cbsMu.Unlock()

	if ns.cbsLink == nil {
		link, err := ns.newCbsLink()
		if err != nil {
			return err
		}
		ns.cbsLink = link
	}
	return nil
}

func (ns *Namespace) negotiateClaim(entityPath string) error {
	err := ns.ensureCbsLink()
	if err != nil {
		return err
	}
	ns.cbsLink.negotiateMu.Lock()
	defer ns.cbsLink.negotiateMu.Unlock()

	name := ns.getEntityAudience(entityPath)
	log.Debugf("sending to: %s, expiring on: %q, via: %s", name, ns.sbToken.Token().ExpiresOn, ns.cbsLink.clientAddress)
	msg := &amqp.Message{
		Value: ns.sbToken.Token().AccessToken,
		Properties: &amqp.MessageProperties{
			ReplyTo: ns.cbsLink.clientAddress,
		},
		ApplicationProperties: map[string]interface{}{
			cbsOperationKey:  cbsOperationPutToken,
			cbsTokenTypeKey:  cbsTokenTypeJwt,
			cbsAudienceKey:   name,
			cbsExpirationKey: ns.sbToken.Token().ExpiresOn,
		},
	}

	_, err = retry(3, 1*time.Second, func() (interface{}, error) {
		log.Debugf("Attempting to negotiate cbs for %s in name %s", entityPath, ns.name)
		err := ns.cbsLink.send(context.Background(), msg)
		if err != nil {
			return nil, err
		}

		res, err := ns.cbsLink.receive(context.Background())
		if err != nil {
			return nil, err
		}

		if statusCode, ok := res.ApplicationProperties[cbsStatusCodeKey].(int32); ok {
			description := res.ApplicationProperties[cbsDescriptionKey].(string)
			if statusCode >= 200 && statusCode < 300 {
				log.Debugf("Successfully negotiated cbs for %s in name %s", entityPath, ns.name)
				return res, nil
			} else if statusCode >= 500 {
				log.Debugf("Re-negotiating cbs for %s in name %s. Received status code: %d and error: %s", entityPath, ns.name, statusCode, description)
				return nil, &retryable{message: "cbs error: " + description}
			} else {
				log.Debugf("Failed negotiating cbs for %s in name %s with error %d and message %s", entityPath, ns.name, statusCode, description)
				return nil, fmt.Errorf("cbs error: failed with code %d and message: %s", statusCode, description)
			}
		}

		return nil, &retryable{message: "cbs error: didn't understand the replied message status code"}
	})

	return err
}

func (cl *cbsLink) forceClose() {
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

func (cl *cbsLink) send(ctx context.Context, msg *amqp.Message) error {
	return cl.sender.Send(ctx, msg)
}

func (cl *cbsLink) receive(ctx context.Context) (*amqp.Message, error) {
	return cl.receiver.Receive(ctx)
}
