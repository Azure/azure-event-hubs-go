// Package cbs provides the functionality for negotiating claims-based security over AMQP for use in Azure Service Bus
// and Event Hubs.
package cbs

import (
	"context"
	"time"

	"github.com/Azure/azure-event-hubs-go/auth"
	"github.com/Azure/azure-event-hubs-go/rpc"
	log "github.com/sirupsen/logrus"
	"pack.ag/amqp"
)

const (
	cbsAddress           = "$cbs"
	cbsOperationKey      = "operation"
	cbsOperationPutToken = "put-token"
	cbsTokenTypeKey      = "type"
	cbsAudienceKey       = "name"
	cbsExpirationKey     = "expiration"
)

// NegotiateClaim attempts to put a token to the $cbs management endpoint to negotiate auth for the given audience
func NegotiateClaim(ctx context.Context, audience string, conn *amqp.Client, provider auth.TokenProvider) error {
	link, err := rpc.NewLink(conn, cbsAddress)
	if err != nil {
		return err
	}
	defer link.Close()

	token, err := provider.GetToken(audience)
	if err != nil {
		return err
	}

	log.Debugf("negotiating claim for audience %s with token type %s and expiry of %s", audience, token.TokenType, token.Expiry)
	msg := &amqp.Message{
		Value: token.Token,
		ApplicationProperties: map[string]interface{}{
			cbsOperationKey:  cbsOperationPutToken,
			cbsTokenTypeKey:  string(token.TokenType),
			cbsAudienceKey:   audience,
			cbsExpirationKey: token.Expiry,
		},
	}

	res, err := link.RetryableRPC(ctx, 3, 1*time.Second, msg)
	if err != nil {
		log.Error(err)
		return err
	}

	log.Debugf("negotiated with response code %d and message: %s", res.Code, res.Description)
	return nil
}
