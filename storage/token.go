package storage

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-event-hubs-go/aad"
	storMgmt "github.com/Azure/azure-sdk-for-go/services/storage/mgmt/2017-10-01/storage"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/date"
)

type (
	// AADSASTokenProvider represents a token provider for Azure Storage SAS using AAD to authorize signing
	AADSASTokenProvider struct {
		ResourceGroup    string
		SubscriptionID   string
		AccountName      string
		aadTokenProvider *adal.ServicePrincipalToken
		tokens           map[string]SASToken
		env              *azure.Environment
		mu               sync.Mutex
	}

	// SASToken contains the expiry time and token for a given SAS
	SASToken struct {
		expiry time.Time
		sas    string
	}

	// AADSASTokenProviderOption provides options for configuring AAD SAS Token Providers
	AADSASTokenProviderOption func(*aad.TokenProviderConfiguration) error
)

// AADSASTokenProviderWithEnvironmentVars configures the TokenProvider using the environment variables available
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
func AADSASTokenProviderWithEnvironmentVars() AADSASTokenProviderOption {
	return func(config *aad.TokenProviderConfiguration) error {
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

// NewAADSASTokenProvider constructs a SAS token provider for Azure storage using Azure Active Directory credentials
func NewAADSASTokenProvider(subscriptionID, resourceGroup, accountName string, opts ...AADSASTokenProviderOption) (*AADSASTokenProvider, error) {
	config := &aad.TokenProviderConfiguration{
		ResourceURI: azure.PublicCloud.ResourceManagerEndpoint,
	}

	for _, opt := range opts {
		err := opt(config)
		if err != nil {
			return nil, err
		}
	}

	spToken, err := config.NewServicePrincipalToken()
	if err != nil {
		return nil, err
	}

	return &AADSASTokenProvider{
		aadTokenProvider: spToken,
		tokens:           make(map[string]SASToken),
		env:              config.Env,
		SubscriptionID:   subscriptionID,
		ResourceGroup:    resourceGroup,
		AccountName:      accountName,
	}, nil
}

// GetToken gets a CBS JWT
func (t *AADSASTokenProvider) GetToken(ctx context.Context, canonicalizedResource string) (SASToken, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	token, ok := t.tokens[canonicalizedResource]
	if ok {
		if token.expiry.Before(time.Now().Add(-5 * time.Minute)) {
			return token, nil
		}
	}
	token, err := t.getToken(ctx, canonicalizedResource)
	if err != nil {
		return SASToken{}, err
	}

	t.tokens[canonicalizedResource] = token
	return token, nil
}

func (t *AADSASTokenProvider) getToken(ctx context.Context, canonicalizedResource string) (SASToken, error) {
	now := &date.Time{}
	expiry := now.Add(1 * time.Hour)
	client := storMgmt.NewAccountsClientWithBaseURI(t.env.ResourceManagerEndpoint, t.SubscriptionID)
	res, err := client.ListServiceSAS(ctx, t.ResourceGroup, t.AccountName, storMgmt.ServiceSasParameters{
		CanonicalizedResource:  &canonicalizedResource,
		Protocols:              "https",
		SharedAccessStartTime:  now,
		SharedAccessExpiryTime: &date.Time{Time: expiry},
	})

	if err != nil {
		return SASToken{}, err
	}

	return SASToken{
		sas:    *res.ServiceSasToken,
		expiry: expiry,
	}, err
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
