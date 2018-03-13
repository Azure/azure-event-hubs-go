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
	eventhubResourceURI = "https://eventhubs.azure.net/"
)

type (
	// TokenProviderConfiguration provides configuration parameters for building JWT AAD providers
	TokenProviderConfiguration struct {
		TenantID            string
		ClientID            string
		ClientSecret        string
		CertificatePath     string
		CertificatePassword string
		ResourceURI         string
		aadToken            *adal.ServicePrincipalToken
		Env                 *azure.Environment
	}

	// TokenProvider provides cbs.TokenProvider functionality for Azure Active Directory JWTs
	TokenProvider struct {
		tokenProvider *adal.ServicePrincipalToken
	}

	// JWTProviderOption provides configuration options for constructing AAD Token Providers
	JWTProviderOption func(provider *TokenProviderConfiguration) error
)

// JWTProviderWithAzureEnvironment configures the token provider to use a specific Azure Environment
func JWTProviderWithAzureEnvironment(env *azure.Environment) JWTProviderOption {
	return func(config *TokenProviderConfiguration) error {
		config.Env = env
		return nil
	}
}

// JWTProviderWithEnvironmentVars configures the TokenProvider using the environment variables available
//
// 1. Client Credentials: attempt to authenticate with a Service Principal via "AZURE_TENANT_ID", "AZURE_CLIENT_ID" and
//    "AZURE_CLIENT_SECRET"
//
// 2. Client Certificate: attempt to authenticate with a Service Principal via "AZURE_TENANT_ID", "AZURE_CLIENT_ID",
//    "AZURE_CERTIFICATE_PATH" and "AZURE_CERTIFICATE_PASSWORD"
//
// 3. Managed Service Identity (MSI): attempt to authenticate via MSI
//
//
// The Azure Environment used can be specified using the name of the Azure Environment set in "AZURE_ENVIRONMENT" var.
func JWTProviderWithEnvironmentVars() JWTProviderOption {
	return func(config *TokenProviderConfiguration) error {
		config.TenantID = os.Getenv("AZURE_TENANT_ID")
		config.ClientID = os.Getenv("AZURE_CLIENT_ID")
		config.ClientSecret = os.Getenv("AZURE_CLIENT_SECRET")
		config.CertificatePath = os.Getenv("AZURE_CERTIFICATE_PATH")
		config.CertificatePassword = os.Getenv("AZURE_CERTIFICATE_PASSWORD")

		if config.Env == nil {
			env, err := azureEnvFromEnvironment()
			if err != nil {
				return err
			}
			config.Env = env
		}
		return nil
	}
}

// JWTProviderWithResourceURI configures the token provider to use a specific eventhubResourceURI URI
func JWTProviderWithResourceURI(resourceURI string) JWTProviderOption {
	return func(config *TokenProviderConfiguration) error {
		config.ResourceURI = resourceURI
		return nil
	}
}

// JWTProviderWithAADToken configures the token provider to use a specific Azure Active Directory Service Principal token
func JWTProviderWithAADToken(aadToken *adal.ServicePrincipalToken) JWTProviderOption {
	return func(config *TokenProviderConfiguration) error {
		config.aadToken = aadToken
		return nil
	}
}

// NewJWTProvider builds an Azure Active Directory claims-based security token provider
func NewJWTProvider(opts ...JWTProviderOption) (auth.TokenProvider, error) {
	config := &TokenProviderConfiguration{
		ResourceURI: eventhubResourceURI,
	}

	for _, opt := range opts {
		err := opt(config)
		if err != nil {
			return nil, err
		}
	}

	if config.aadToken == nil {
		spToken, err := config.NewServicePrincipalToken()
		if err != nil {
			return nil, err
		}
		config.aadToken = spToken
	}
	return &TokenProvider{tokenProvider: config.aadToken}, nil
}

// NewServicePrincipalToken creates a new Azure Active Directory Service Principal token provider
func (c *TokenProviderConfiguration) NewServicePrincipalToken() (*adal.ServicePrincipalToken, error) {
	oauthConfig, err := adal.NewOAuthConfig(c.Env.ActiveDirectoryEndpoint, c.TenantID)
	if err != nil {
		return nil, err
	}

	// 1.Client Credentials
	if c.ClientSecret != "" {
		log.Debug("creating a token via a service principal client secret")
		spToken, err := adal.NewServicePrincipalToken(*oauthConfig, c.ClientID, c.ClientSecret, c.ResourceURI)
		if err != nil {
			return nil, fmt.Errorf("failed to get oauth token from client credentials: %v", err)
		}
		if err := spToken.Refresh(); err != nil {
			return nil, fmt.Errorf("failed to refersh token: %v", spToken)
		}
		return spToken, nil
	}

	// 2. Client Certificate
	if c.CertificatePath != "" {
		log.Debug("creating a token via a service principal client certificate")
		certData, err := ioutil.ReadFile(c.CertificatePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read the certificate file (%s): %v", c.CertificatePath, err)
		}
		certificate, rsaPrivateKey, err := decodePkcs12(certData, c.CertificatePassword)
		if err != nil {
			return nil, fmt.Errorf("failed to decode pkcs12 certificate while creating spt: %v", err)
		}
		spToken, err := adal.NewServicePrincipalTokenFromCertificate(*oauthConfig, c.ClientID, certificate, rsaPrivateKey, c.ResourceURI)
		if err != nil {
			return nil, fmt.Errorf("failed to get oauth token from certificate auth: %v", err)
		}
		if err := spToken.Refresh(); err != nil {
			return nil, fmt.Errorf("failed to refersh token: %v", spToken)
		}
		return spToken, nil
	}

	// 3. By default return MSI
	log.Debug("creating a token via MSI")
	msiEndpoint, err := adal.GetMSIVMEndpoint()
	if err != nil {
		return nil, err
	}
	spToken, err := adal.NewServicePrincipalTokenFromMSI(msiEndpoint, c.ResourceURI)
	if err != nil {
		return nil, fmt.Errorf("failed to get oauth token from MSI: %v", err)
	}
	if err := spToken.Refresh(); err != nil {
		return nil, fmt.Errorf("failed to refersh token: %v", spToken)
	}
	return spToken, nil
}

// GetToken gets a CBS JWT
func (t *TokenProvider) GetToken(audience string) (*auth.Token, error) {
	token := t.tokenProvider.Token()
	expireTicks, err := strconv.ParseInt(token.ExpiresOn, 10, 64)
	if err != nil {
		log.Debugf("%v", token.AccessToken)
		return nil, err
	}
	expires := time.Unix(expireTicks, 0)

	if expires.Before(time.Now()) {
		log.Debug("refreshing AAD token since it has expired")
		if err := t.tokenProvider.Refresh(); err != nil {
			log.Error("refreshing AAD token has failed")
			return nil, err
		}
		token = t.tokenProvider.Token()
		log.Debug("refreshing AAD token has succeeded")
	}

	return auth.NewToken(auth.CBSTokenTypeJWT, token.AccessToken, token.ExpiresOn), nil
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

func azureEnvFromEnvironment() (*azure.Environment, error) {
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
	return &env, nil
}
