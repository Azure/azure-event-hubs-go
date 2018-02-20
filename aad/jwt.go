package aad

import (
	"crypto/rsa"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/Azure/azure-event-hubs-go/auth"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/pkcs12"
)

const (
	resource = "https://eventhubs.azure.net/"
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

// NewProviderFromEnvironment builds a new TokenProvider using environment variable available
//
// 1. Client Credentials: attempt to authenticate with a Service Principal via "AZURE_TENANT_ID", "AZURE_CLIENT_ID" and
//    "AZURE_CLIENT_SECRET"
//
// 2. Client Certificate: attempt to authenticate with a Service Principal via "AZURE_TENANT_ID", "AZURE_CLIENT_ID",
//    "AZURE_CERTIFICATE_PATH" and "AZURE_CERTIFICATE_PASSWORD"
//
// 3. Managed Service Identity (MSI): attempt to authenticate via MSI
func NewProviderFromEnvironment() (auth.TokenProvider, error) {
	tenantID := os.Getenv("AZURE_TENANT_ID")
	clientID := os.Getenv("AZURE_CLIENT_ID")
	clientSecret := os.Getenv("AZURE_CLIENT_SECRET")
	certificatePath := os.Getenv("AZURE_CERTIFICATE_PATH")
	certificatePassword := os.Getenv("AZURE_CERTIFICATE_PASSWORD")
	envName := os.Getenv("AZURE_ENVIRONMENT")

	var env azure.Environment
	if envName == "" {
		env = azure.PublicCloud
	} else {
		var err error
		env, err = azure.EnvironmentFromName(envName)
		if err != nil {
			return nil, err
		}
	}

	oauthConfig, err := adal.NewOAuthConfig(env.ActiveDirectoryEndpoint, tenantID)
	if err != nil {
		return nil, err
	}

	// 1.Client Credentials
	if clientSecret != "" {
		log.Debug("creating a token via a service principal client secret")
		spToken, err := adal.NewServicePrincipalToken(*oauthConfig, clientID, clientSecret, resource)
		if err != nil {
			return nil, fmt.Errorf("failed to get oauth token from client credentials: %v", err)
		}
		if err := spToken.Refresh(); err != nil {
			return nil, fmt.Errorf("failed to refersh token: %v", spToken)
		}
		return NewProvider(spToken), nil
	}

	// 2. Client Certificate
	if certificatePath != "" {
		log.Debug("creating a token via a service principal client certificate")
		certData, err := ioutil.ReadFile(certificatePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read the certificate file (%s): %v", certificatePath, err)
		}
		certificate, rsaPrivateKey, err := decodePkcs12(certData, certificatePassword)
		if err != nil {
			return nil, fmt.Errorf("failed to decode pkcs12 certificate while creating spt: %v", err)
		}
		spToken, err := adal.NewServicePrincipalTokenFromCertificate(*oauthConfig, clientID, certificate, rsaPrivateKey, resource)
		if err != nil {
			return nil, fmt.Errorf("failed to get oauth token from certificate auth: %v", err)
		}
		if err := spToken.Refresh(); err != nil {
			return nil, fmt.Errorf("failed to refersh token: %v", spToken)
		}
		return NewProvider(spToken), nil
	}

	// 3. By default return MSI
	log.Debug("creating a token via MSI")
	msiEndpoint, err := adal.GetMSIVMEndpoint()
	if err != nil {
		return nil, err
	}
	spToken, err := adal.NewServicePrincipalTokenFromMSI(msiEndpoint, resource)
	if err != nil {
		return nil, fmt.Errorf("failed to get oauth token from MSI: %v", err)
	}
	if err := spToken.Refresh(); err != nil {
		return nil, fmt.Errorf("failed to refersh token: %v", spToken)
	}
	return NewProvider(spToken), nil
}

// GetToken gets a CBS JWT token
func (t *TokenProvider) GetToken(audience string) (*auth.Token, error) {
	token := t.tokenProvider.Token()
	expireTicks, err := strconv.Atoi(token.ExpiresOn)
	if err != nil {
		log.Debugf("%v", token.AccessToken)
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

func decodePkcs12(pkcs []byte, password string) (*x509.Certificate, *rsa.PrivateKey, error) {
	privateKey, certificate, err := pkcs12.Decode(pkcs, password)
	if err != nil {
		return nil, nil, err
	}

	rsaPrivateKey, isRsaKey := privateKey.(*rsa.PrivateKey)
	if !isRsaKey {
		return nil, nil, fmt.Errorf("PKCS#12 certificate must contain an RSA private key")
	}

	return certificate, rsaPrivateKey, nil
}
