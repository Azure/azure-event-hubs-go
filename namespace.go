package eventhub

import (
	"fmt"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	log "github.com/sirupsen/logrus"
	"pack.ag/amqp"
	"sync"
)

type (
	// Namespace provides a simplified facade over the AMQP implementation of an Azure Event Hub Namespace.
	Namespace struct {
		client           *amqp.Client
		clientMu         sync.Mutex
		armToken         *adal.ServicePrincipalToken
		sbToken          *adal.ServicePrincipalToken
		environment      azure.Environment
		connectionString string
		subscriptionID   string
		resourceGroup    string
		name             string
		primaryKey       string
		Logger           *log.Logger
		cbsMu            sync.Mutex
		cbsLink          *cbsLink
	}

	// ServicePrincipalCredentials contains the details needed to authenticate to Azure Active Directory with a Service
	// Principal. For more info on Service Principals see: https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal
	ServicePrincipalCredentials struct {
		TenantID      string
		ApplicationID string
		Secret        string
	}

	// EntityManager provides the ability to manage Event Hub entities
	EntityManager interface {
	}
)

// NewNamespaceWithServicePrincipalCredentials builds an Event Hubs Namespace which authenticates with Azure Active Directory
// using Claims-based Security
func NewNamespaceWithServicePrincipalCredentials(subscriptionID, resourceGroup, name string, credentials ServicePrincipalCredentials, env azure.Environment) (*Namespace, error) {
	armToken, err := getArmTokenProvider(credentials, env)
	if err != nil {
		return nil, err
	}

	sbToken, err := getEventHubsTokenProvider(credentials, env)
	if err != nil {
		return nil, err
	}

	return NewNamespaceWithTokenProviders(subscriptionID, resourceGroup, name, armToken, sbToken, env)
}

// NewNamespaceWithTokenProviders builds an Event Hub Namespace which authenticates with Azure Active Directory
// using Claims-based Security using Azure Active Directory token providers
func NewNamespaceWithTokenProviders(subscriptionID, resourceGroup, name string, armToken, serviceBusToken *adal.ServicePrincipalToken, env azure.Environment) (*Namespace, error) {
	ns := &Namespace{
		name:           name,
		sbToken:        serviceBusToken,
		armToken:       armToken,
		subscriptionID: subscriptionID,
		resourceGroup:  resourceGroup,
		environment:    env,
		Logger:         log.New(),
	}
	ns.Logger.SetLevel(log.WarnLevel)

	return ns, nil
}

// NewEventHub builds an instance of an EventHub for sending and receiving messages
func (ns *Namespace) NewEventHub(name string) SenderReceiver {
	return &hub{
		name:      name,
		namespace: ns,
	}
}

func (ns *Namespace) connection() (*amqp.Client, error) {
	ns.clientMu.Lock()
	defer ns.clientMu.Unlock()

	if ns.client == nil && ns.claimsBasedSecurityEnabled() {
		host := ns.getAmqpHostURI()
		client, err := amqp.Dial(host, amqp.ConnSASLAnonymous(), amqp.ConnMaxSessions(65535))
		if err != nil {
			return nil, err
		}
		ns.client = client
	}
	return ns.client, nil
}

func (ns *Namespace) getAmqpHostURI() string {
	return fmt.Sprintf("amqps://%s.%s/", ns.name, ns.environment.ServiceBusEndpointSuffix)
}

func (ns *Namespace) getEntityAudience(entityPath string) string {
	return ns.getAmqpHostURI() + entityPath
}

func getArmTokenProvider(credential ServicePrincipalCredentials, env azure.Environment) (*adal.ServicePrincipalToken, error) {
	return getTokenProvider(env.ResourceManagerEndpoint, credential, env)
}

func getEventHubsTokenProvider(credential ServicePrincipalCredentials, env azure.Environment) (*adal.ServicePrincipalToken, error) {
	return getTokenProvider(env.ServiceBusEndpoint, credential, env)
}

// claimsBasedSecurityEnabled indicates that the connection will use AAD JWT RBAC to authenticate in connections
func (ns *Namespace) claimsBasedSecurityEnabled() bool {
	return ns.sbToken != nil
}

func getTokenProvider(resourceURI string, cred ServicePrincipalCredentials, env azure.Environment) (*adal.ServicePrincipalToken, error) {
	oauthConfig, err := adal.NewOAuthConfig(env.ActiveDirectoryEndpoint, cred.TenantID)
	if err != nil {
		log.Fatalln(err)
	}

	tokenProvider, err := adal.NewServicePrincipalToken(*oauthConfig, cred.ApplicationID, cred.Secret, resourceURI)
	if err != nil {
		return nil, err
	}

	err = tokenProvider.Refresh()
	if err != nil {
		return nil, err
	}

	return tokenProvider, nil
}
