package sas

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-event-hubs-go/auth"
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
