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
	"github.com/Azure/azure-event-hubs-go/common"
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
)

// NewProvider builds a SAS claims-based security token provider
func NewProvider(namespace, keyName, key string) auth.TokenProvider {
	return &TokenProvider{
		signer: NewSigner(namespace, keyName, key),
	}
}

// NewProviderFromEnvironment creates a new SAS TokenProvider from environment variables
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
func NewProviderFromEnvironment() (auth.TokenProvider, error) {
	connStr := os.Getenv("EVENTHUB_CONNECTION_STRING")
	if connStr != "" {
		parsed, err := common.ParsedConnectionFromStr(connStr)
		if err != nil {
			return nil, err
		}
		return NewProvider(parsed.Namespace, parsed.KeyName, parsed.Key), nil
	}

	var (
		keyName   = os.Getenv("EVENTHUB_KEY_NAME")
		keyValue  = os.Getenv("EVENTHUB_KEY_VALUE")
		namespace = os.Getenv("EVENTHUB_NAMESPACE")
	)

	if keyName == "" || keyValue == "" || namespace == "" {
		return nil, errors.New("unable to build SAS token provider because (EVENTHUB_KEY_NAME, EVENTHUB_KEY_VALUE and EVENTHUB_NAMESPACE) were empty, and EVENTHUB_CONNECTION_STRING was empty")
	}

	return NewProvider(namespace, keyName, keyValue), nil
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
