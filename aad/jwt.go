package aad

import (
	"github.com/Azure/azure-event-hubs-go/auth"
	"github.com/Azure/go-autorest/autorest/adal"
	log "github.com/sirupsen/logrus"
	"strconv"
	"time"
)

type (
	// TokenProvider provides cbs.TokenProvider functionality for Azure Active Directory JWT tokens
	TokenProvider struct {
		tokenProvider *adal.ServicePrincipalToken
	}
)

// NewProvider builds an Azure Active Directory claims-based security token provider
func NewProvider(tokenProvider *adal.ServicePrincipalToken) auth.TokenProvider {
	return &TokenProvider{
		tokenProvider: tokenProvider,
	}
}

// GetToken gets a CBS JWT token
func (t *TokenProvider) GetToken(audience string) (*auth.Token, error) {
	token := t.tokenProvider.Token()
	expireTicks, err := strconv.Atoi(token.ExpiresOn)
	if err != nil {
		return nil, err
	}
	currentTicks := time.Now().UTC().Unix()
	if int64(expireTicks) < currentTicks {
		log.Debug("refreshing AAD token since it has expired")
		if err := t.tokenProvider.Refresh(); err != nil {
			log.Error("refreshing AAD token has failed")
			return nil, err
		}
		token = t.tokenProvider.Token()
		log.Debug("refreshing AAD token has succeeded")
	}

	return auth.NewToken(auth.CbsTokenTypeJwt, token.AccessToken, token.ExpiresOn), nil
}
