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
// Expected Environment Variables:
//   - "EVENTHUB_NAMESPACE" the namespace of the Event Hub instance
//   - "EVENTHUB_KEY_NAME" the name of the Event Hub key
//   - "EVENTHUB_KEY_VALUE" the secret for the Event Hub key named in "EVENTHUB_KEY_NAME"
func NewProviderFromEnvironment() (auth.TokenProvider, error) {
	keyName := os.Getenv("EVENTHUB_KEY_NAME")
	keyValue := os.Getenv("EVENTHUB_KEY_VALUE")
	namespace := os.Getenv("EVENTHUB_NAMESPACE")

	if keyName == "" || keyValue == "" || namespace == "" {
		return nil, errors.New("one or more environment variables, EVENTHUB_KEY_NAME, EVENTHUB_KEY_VALUE or EVENTHUB_NAMESPACE were empty")
	}
	return NewProvider(namespace, keyName, keyValue), nil
}

// GetToken gets a CBS SAS token
func (t *TokenProvider) GetToken(audience string) (*auth.Token, error) {
	signed, expiry := t.signer.SignWithDuration(audience, 2*time.Hour)
	return auth.NewToken(auth.CbsTokenTypeSas, signed, expiry), nil
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
	u := strings.ToLower(url.QueryEscape(uri))
	sts := stringToSign(u, expiry)
	sig := s.signString(sts)
	return fmt.Sprintf("SharedAccessSignature sig=%s&se=%s&skn=%s&sr=%s", sig, expiry, s.keyName, u)
}

func signatureExpiry(from time.Time, interval time.Duration) string {
	t := from.Add(interval).Round(time.Second).Unix()
	return strconv.Itoa(int(t))
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
