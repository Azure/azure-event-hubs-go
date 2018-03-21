// Package sas provides SAS token functionality which implements TokenProvider from package auth for use with Azure
// Event Hubs and Service Bus.
package sas

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-event-hubs-go/auth"
	"github.com/Azure/azure-event-hubs-go/internal/common"
	"github.com/pkg/errors"
)

type (
	// Signer provides SAS token generation for use in Service Bus and Event Hub
	Signer struct {
		namespace string
		keyName   string
		key       string
	}

	// TokenProvider is a SAS claims-based security token provider
	TokenProvider struct {
		signer *Signer
	}

	// TokenProviderOption provides configuration options for SAS Token Providers
	TokenProviderOption func(*TokenProvider) error
)

// TokenProviderWithEnvironmentVars creates a new SAS TokenProvider from environment variables
//
// There are two sets of environment variables which can produce a SAS TokenProvider
//
// 1) Expected Environment Variables:
//   - "EVENTHUB_NAMESPACE" the namespace of the Event Hub instance
//   - "EVENTHUB_KEY_NAME" the name of the Event Hub key
//   - "EVENTHUB_KEY_VALUE" the secret for the Event Hub key named in "EVENTHUB_KEY_NAME"
//
// 2) Expected Environment Variable:
//   - "EVENTHUB_CONNECTION_STRING" connection string from the Azure portal
//
// looks like: Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=superSecret1234=
func TokenProviderWithEnvironmentVars() TokenProviderOption {
	return func(provider *TokenProvider) error {
		connStr := os.Getenv("EVENTHUB_CONNECTION_STRING")
		if connStr != "" {
			parsed, err := common.ParsedConnectionFromStr(connStr)
			if err != nil {
				return err
			}
			provider.signer = NewSigner(parsed.Namespace, parsed.KeyName, parsed.Key)
			return nil
		}

		var (
			keyName   = os.Getenv("EVENTHUB_KEY_NAME")
			keyValue  = os.Getenv("EVENTHUB_KEY_VALUE")
			namespace = os.Getenv("EVENTHUB_NAMESPACE")
		)

		if keyName == "" || keyValue == "" || namespace == "" {
			return errors.New("unable to build SAS token provider because (EVENTHUB_KEY_NAME, EVENTHUB_KEY_VALUE and EVENTHUB_NAMESPACE) were empty, and EVENTHUB_CONNECTION_STRING was empty")
		}
		provider.signer = NewSigner(namespace, keyName, keyValue)
		return nil
	}
}

// TokenProviderWithNamespaceAndKey configures a SAS TokenProvider to use the given namespace and key combination for signing
func TokenProviderWithNamespaceAndKey(namespace, keyName, key string) TokenProviderOption {
	return func(provider *TokenProvider) error {
		provider.signer = NewSigner(namespace, keyName, key)
		return nil
	}
}

// NewTokenProvider builds a SAS claims-based security token provider
func NewTokenProvider(opts ...TokenProviderOption) (auth.TokenProvider, error) {
	provider := new(TokenProvider)
	for _, opt := range opts {
		err := opt(provider)
		if err != nil {
			return nil, err
		}
	}
	return provider, nil
}

// GetToken gets a CBS SAS token
func (t *TokenProvider) GetToken(audience string) (*auth.Token, error) {
	signed, expiry := t.signer.SignWithDuration(audience, 2*time.Hour)
	return auth.NewToken(auth.CBSTokenTypeSAS, signed, expiry), nil
}

// NewSigner builds a new SAS signer for use in generation Service Bus and Event Hub SAS tokens
func NewSigner(namespace, keyName, key string) *Signer {
	return &Signer{
		namespace: namespace,
		keyName:   keyName,
		key:       key,
	}
}

// SignWithDuration signs a given for a period of time from now
func (s *Signer) SignWithDuration(uri string, interval time.Duration) (signed, expiry string) {
	expiry = signatureExpiry(time.Now(), interval)
	return s.SignWithExpiry(uri, expiry), expiry
}

// SignWithExpiry signs a given uri with a given expiry string
func (s *Signer) SignWithExpiry(uri, expiry string) string {
	audience := strings.ToLower(url.QueryEscape(uri))
	sts := stringToSign(audience, expiry)
	sig := s.signString(sts)
	return fmt.Sprintf("SharedAccessSignature sr=%s&sig=%s&se=%s&skn=%s", audience, sig, expiry, s.keyName)
}

func signatureExpiry(from time.Time, interval time.Duration) string {
	t := from.Add(interval).Round(time.Second).Unix()
	return strconv.FormatInt(t, 10)
}

func stringToSign(uri, expiry string) string {
	return uri + "\n" + expiry
}

func (s *Signer) signString(str string) string {
	h := hmac.New(sha256.New, []byte(s.key))
	h.Write([]byte(str))
	encodedSig := base64.StdEncoding.EncodeToString(h.Sum(nil))
	return url.QueryEscape(encodedSig)
}
