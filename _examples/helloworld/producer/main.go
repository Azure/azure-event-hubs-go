package main

import (
	"github.com/Azure/azure-event-hubs-go"
	"fmt"
	"os"
	"github.com/Azure/go-autorest/autorest/azure"
	"log"
	"context"
	"pack.ag/amqp"
	"bufio"
)

const (
	Location          = "eastus"
	ResourceGroupName = "ehtest"
	HubName           = "producerConsumer"
)

func main() {
	hub, _ := initHub()

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter text: ")
		text, _ := reader.ReadString('\n')
		hub.Send(context.Background(), &amqp.Message{Data: []byte(text)})
		if text == "exit\n" {
			break
		}
	}
}

func initHub() (eventhub.SenderReceiver, *[]string) {
	subscriptionID := mustGetenv("AZURE_SUBSCRIPTION_ID")
	namespace := mustGetenv("EVENTHUB_NAMESPACE")
	creds := eventhub.ServicePrincipalCredentials{
		TenantID:      mustGetenv("AZURE_TENANT_ID"),
		ApplicationID: mustGetenv("AZURE_CLIENT_ID"),
		Secret:        mustGetenv("AZURE_CLIENT_SECRET"),
	}
	ns, err := eventhub.NewNamespaceWithServicePrincipalCredentials(subscriptionID, ResourceGroupName, namespace, creds, azure.PublicCloud)
	if err != nil {
		panic(err)
	}

	hubMgmt, err := ns.EnsureEventHub(context.Background(), HubName)
	if err != nil {
		log.Fatal(err)
	}

	hub, err := ns.NewEventHub(HubName)
	if err != nil {
		log.Fatal(err)
	}
	return hub, hubMgmt.PartitionIds
}

func mustGetenv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic("Environment variable '" + key + "' required for integration tests.")
	}
	return v
}
