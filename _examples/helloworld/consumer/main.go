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

	for _, partitionID := range *partitions {
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
