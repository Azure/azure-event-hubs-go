package eventhub

import (
	"context"
	mgmt "github.com/Azure/azure-sdk-for-go/services/eventhub/mgmt/2017-04-01/eventhub"
	rm "github.com/Azure/azure-sdk-for-go/services/resources/mgmt/2017-05-10/resources"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	"net/http"
)

type (
	// HubMgmtOption represents an option for configuring an Event Hub.
	HubMgmtOption func(model *mgmt.Model) error
	// NamespaceMgmtOption represents an option for configuring a Namespace
	NamespaceMgmtOption func(ns *mgmt.EHNamespace) error
)

// GetNamespace fetches a namespace entity from the Azure Resource Manager
func (ns *Namespace) GetNamespace(ctx context.Context) (*mgmt.EHNamespace, error) {
	client := ns.getNamespaceMgmtClient()
	nsInstance, err := client.Get(ctx, ns.resourceGroup, ns.name)
	if err != nil {
		return nil, err
	}
	return &nsInstance, nil
}

// EnsureResourceGroup creates a Azure Resource Group if it does not already exist
func EnsureResourceGroup(ctx context.Context, subscriptionID, name, location string, armToken *adal.ServicePrincipalToken, env azure.Environment) (*rm.Group, error) {
	groupClient := getRmGroupClientWithToken(subscriptionID, armToken, env)
	group, err := groupClient.Get(ctx, name)

	if group.StatusCode == http.StatusNotFound {
		group, err = groupClient.CreateOrUpdate(ctx, name, rm.Group{Location: ptrString(location)})
		if err != nil {
			return nil, err
		}
	} else if group.StatusCode >= 400 {
		return nil, err
	}

	return &group, nil
}

// EnsureNamespace creates a Azure Event Hub Namespace if it does not already exist
func EnsureNamespace(ctx context.Context, subscriptionID, rg, name, location string, armToken *adal.ServicePrincipalToken, env azure.Environment, opts ...NamespaceMgmtOption) (*mgmt.EHNamespace, error) {
	_, err := EnsureResourceGroup(ctx, subscriptionID, rg, location, armToken, env)
	if err != nil {
		return nil, err
	}

	client := getNamespaceMgmtClientWithToken(subscriptionID, armToken, env)
	namespace, err := client.Get(ctx, rg, name)
	if err != nil {
		return nil, err
	}

	if namespace.StatusCode == 404 {
		newNamespace := &mgmt.EHNamespace{
			Name: &name,

			Sku: &mgmt.Sku{
				Name:     mgmt.Basic,
				Tier:     mgmt.SkuTierBasic,
				Capacity: ptrInt32(1),
			},
			EHNamespaceProperties: &mgmt.EHNamespaceProperties{
				IsAutoInflateEnabled:   ptrBool(false),
				MaximumThroughputUnits: ptrInt32(1),
			},
		}

		for _, opt := range opts {
			err = opt(newNamespace)
			if err != nil {
				return nil, err
			}
		}

		nsFuture, err := client.CreateOrUpdate(ctx, rg, name, *newNamespace)
		if err != nil {
			return nil, err
		}

		namespace, err = nsFuture.Result(*client)
		if err != nil {
			return nil, err
		}
	} else if namespace.StatusCode >= 400 {
		return nil, err
	}

	return &namespace, nil
}

// EnsureEventHub creates an Event Hub within the given Namespace if it does not already exist
func (ns *Namespace) EnsureEventHub(ctx context.Context, name string, opts ...HubMgmtOption) (*mgmt.Model, error) {
	client := ns.getEventHubMgmtClient()
	hub, err := client.Get(ctx, ns.resourceGroup, ns.name, name)

	if err != nil {
		newHub := &mgmt.Model{
			Name:       &name,
			Properties: &mgmt.Properties{},
		}

		for _, opt := range opts {
			err = opt(newHub)
			if err != nil {
				return nil, err
			}
		}

		hub, err = client.CreateOrUpdate(ctx, ns.resourceGroup, ns.name, name, *newHub)
		if err != nil {
			return nil, err
		}
	}
	return &hub, nil
}

// DeleteEventHub deletes an Event Hub within the given Namespace
func (ns *Namespace) DeleteEventHub(ctx context.Context, name string) error {
	client := ns.getEventHubMgmtClient()
	_, err := client.Delete(ctx, ns.resourceGroup, ns.name, name)
	if err != nil {
		return err
	}
	return nil
}

func (ns *Namespace) getEventHubMgmtClient() *mgmt.EventHubsClient {
	client := mgmt.NewEventHubsClientWithBaseURI(ns.environment.ResourceManagerEndpoint, ns.subscriptionID)
	client.Authorizer = autorest.NewBearerAuthorizer(ns.armToken)
	return &client
}

func (ns *Namespace) getNamespaceMgmtClient() *mgmt.NamespacesClient {
	return getNamespaceMgmtClientWithToken(ns.subscriptionID, ns.armToken, ns.environment)
}

func getNamespaceMgmtClientWithToken(subscriptionID string, armToken *adal.ServicePrincipalToken, env azure.Environment) *mgmt.NamespacesClient {
	client := mgmt.NewNamespacesClientWithBaseURI(env.ResourceManagerEndpoint, subscriptionID)
	client.Authorizer = autorest.NewBearerAuthorizer(armToken)
	return &client
}

func (ns *Namespace) getNamespaceMgmtClientWithCredentials(ctx context.Context, subscriptionID, rg, name string, credentials *ServicePrincipalCredentials) *mgmt.NamespacesClient {
	client := mgmt.NewNamespacesClientWithBaseURI(ns.environment.ResourceManagerEndpoint, ns.subscriptionID)
	client.Authorizer = autorest.NewBearerAuthorizer(ns.armToken)
	return &client
}

func (ns *Namespace) getRmGroupClient() *rm.GroupsClient {
	return getRmGroupClientWithToken(ns.subscriptionID, ns.armToken, ns.environment)
}

func getRmGroupClientWithToken(subscriptionID string, armToken *adal.ServicePrincipalToken, env azure.Environment) *rm.GroupsClient {
	groupsClient := rm.NewGroupsClientWithBaseURI(env.ResourceManagerEndpoint, subscriptionID)
	groupsClient.Authorizer = autorest.NewBearerAuthorizer(armToken)
	return &groupsClient
}
