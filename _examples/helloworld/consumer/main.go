package main

import (
	"github.com/Azure/azure-event-hubs-go"
	"fmt"
	"time"
	"os"
	"github.com/Azure/go-autorest/autorest/azure"
	"log"
	"context"
	"pack.ag/amqp"
	"github.com/Azure/azure-event-hubs-go/aad"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest"
	mgmt "github.com/Azure/azure-sdk-for-go/services/eventhub/mgmt/2017-04-01/eventhub"
)

const (
	Location          = "eastus"
	ResourceGroupName = "ehtest"
	HubName           = "producerConsumer"
)

func main() {
	hub, partitions := initHub()
	exit := make(chan struct{})

	handler := func(ctx context.Context, msg *amqp.Message) error {
		text := string(msg.Data)
		if text == "exit\n" {
			fmt.Println("Someone told me to exit!")
			exit <- *new(struct{})
		} else {
			fmt.Println(string(msg.Data))
		}
		return nil
	}

	for _, partitionID := range partitions {
		hub.Receive(partitionID, handler)
	}

	select {
	case <-exit:
		fmt.Println("closing after 2 seconds")
		select {
		case <-time.After(2 * time.Second):
			return
		}
	}
}

func initHub() (eventhub.Client, []string) {
	namespace := mustGetenv("EVENTHUB_NAMESPACE")
	hubMgmt, err := ensureEventHub(context.Background(), HubName)
	if err != nil {
		log.Fatal(err)
	}

	aadToken, err := getEventHubsTokenProvider()
	if err != nil {
		log.Fatal(err)
	}
	provider := aad.NewProvider(aadToken)
	hub, err := eventhub.NewClient(namespace, HubName, provider)
	if err != nil {
		panic(err)
	}
	return hub, *hubMgmt.PartitionIds
}

func mustGetenv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic("Environment variable '" + key + "' required for integration tests.")
	}
	return v
}

func getEventHubsTokenProvider() (*adal.ServicePrincipalToken, error) {
	// TODO: fix the azure environment var for the SB endpoint and EH endpoint
	return getTokenProvider("https://eventhubs.azure.net/")
}

func getTokenProvider(resourceURI string) (*adal.ServicePrincipalToken, error) {
	oauthConfig, err := adal.NewOAuthConfig(azure.PublicCloud.ActiveDirectoryEndpoint, mustGetenv("AZURE_TENANT_ID"))
	if err != nil {
		log.Fatalln(err)
	}

	tokenProvider, err := adal.NewServicePrincipalToken(*oauthConfig, mustGetenv("AZURE_CLIENT_ID"), mustGetenv("AZURE_CLIENT_SECRET"), resourceURI)
	if err != nil {
		return nil, err
	}

	err = tokenProvider.Refresh()
	if err != nil {
		return nil, err
	}

	return tokenProvider, nil
}

func ensureEventHub(ctx context.Context, name string) (*mgmt.Model, error) {
	namespace := mustGetenv("EVENTHUB_NAMESPACE")
	client := getEventHubMgmtClient()
	hub, err := client.Get(ctx, ResourceGroupName, namespace, name)

	partitionCount := int64(4)
	if err != nil {
		newHub := &mgmt.Model{
			Name: &name,
			Properties: &mgmt.Properties{
				PartitionCount: &partitionCount,
			},
		}

		hub, err = client.CreateOrUpdate(ctx, ResourceGroupName, namespace, name, *newHub)
		if err != nil {
			return nil, err
		}
	}
	return &hub, nil
}

func getEventHubMgmtClient() *mgmt.EventHubsClient {
	subID := mustGetenv("AZURE_SUBSCRIPTION_ID")
	client := mgmt.NewEventHubsClientWithBaseURI(azure.PublicCloud.ResourceManagerEndpoint, subID)
	armToken, err := getTokenProvider(azure.PublicCloud.ResourceManagerEndpoint)
	if err != nil {
		log.Fatal(err)
	}
	client.Authorizer = autorest.NewBearerAuthorizer(armToken)
	return &client
}
