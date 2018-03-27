# Microsoft Azure Event Hubs Client for Golang
[![Go Report Card](https://goreportcard.com/badge/github.com/Azure/azure-event-hubs-go)](https://goreportcard.com/report/github.com/Azure/azure-event-hubs-go)
[![godoc](https://godoc.org/github.com/Azure/azure-event-hubs-go?status.svg)](https://godoc.org/github.com/Azure/azure-event-hubs-go)
[![Build Status](https://travis-ci.org/Azure/azure-event-hubs-go.svg?branch=master)](https://travis-ci.org/Azure/azure-event-hubs-go)

Azure Event Hubs is a highly scalable publish-subscribe service that can ingest millions of events per second and 
stream them into multiple applications. This lets you process and analyze the massive amounts of data produced by your 
connected devices and applications. Once Event Hubs has collected the data, you can retrieve, transform and store it by 
using any real-time analytics provider or with batching/storage adapters. 

Refer to the [online documentation](https://azure.microsoft.com/services/event-hubs/) to learn more about Event Hubs in 
general.

This library is a pure Golang implementation of Azure Event Hubs over AMQP.


## Preview of Event Hubs for Golang
This library is currently a preview. There may be breaking interface changes until it reaches semantic version `v1.0.0`. 
If you run into an issue, please don't hesitate to log a 
[new issue](https://github.com/Azure/azure-event-hubs-go/issues/new) or open a pull request.

## Installing the library
To more reliably manage dependencies in your application we recommend [golang/dep](https://github.com/golang/dep).

With dep:
```
dep ensure -add github.com/Azure/azure-event-hubs-go
```

With go get:
```
go get -u github.com/Azure/azure-event-hubs-go/...
```

If you need to install Go, follow [the official instructions](https://golang.org/dl/)

## Using Event Hubs
In this section we'll cover some basics of the library to help you get started.

This library has two main dependencies, [vcabbage/amqp](https://github.com/vcabbage/amqp) and 
[Azure AMQP Common](https://github.com/Azure/azure-amqp-common-go). The former provides the AMQP protocol implementation
and the latter provides some common authentication, persistence and request-response message flows.

### Quick start
Let's send and receive `"hello, world!"`.
```go
// create a new Event Hub from environment variables
// the go docs for the func have a full description of the environment variables
hub, err := eventhub.NewHubFromEnvironment()
if err != nil {
    // handle err
}

ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
defer cancel()
// send a single message into a random partition
hub.Send(ctx, eventhub.NewEventFromString("hello, world!"))

handler := func(c context.Context, event *eventhub.Event) error {
	fmt.Println(string(event.Data))
	return nil
}

// listen to each partition of the Event Hub
runtimeInfo, err := hub.GetRuntimeInformation(ctx)
for _, partitionID := range runtimeInfo.PartitionIDs {
	// start receiving messages -- Receive is non-blocking and starts immediately
	_, err := hub.Receive(ctx, partitionID, handler, eventhub.ReceiveWithLatestOffset())
	if err != nil {
		// handle err
	}
}

// Wait for a signal to quit:
signalChan := make(chan os.Signal, 1)
signal.Notify(signalChan, os.Interrupt, os.Kill)
<-signalChan
return hub.Close()
```

### Environment Variables
In the above example, the `Hub` instance was created using environment variables. Here is a list of environment 
variables used in this project.

#### Event Hub env vars
- "EVENTHUB_NAMESPACE" the namespace of the Event Hub instance
- "EVENTHUB_NAME" the name of the Event Hub instance

#### SAS TokenProvider environment variables:
There are two sets of environment variables which can produce a SAS TokenProvider
1) Expected Environment Variables:
    - "EVENTHUB_NAMESPACE" the namespace of the Event Hub instance
    - "EVENTHUB_KEY_NAME" the name of the Event Hub key
    - "EVENTHUB_KEY_VALUE" the secret for the Event Hub key named in "EVENTHUB_KEY_NAME"

2) Expected Environment Variable:
    - "EVENTHUB_CONNECTION_STRING" connection string from the Azure portal like: `Endpoint=sb://foo.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fluffypuppy`

#### AAD TokenProvider environment variables:
1) Client Credentials: attempt to authenticate with a [Service Principal](https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal) via
    - "AZURE_TENANT_ID" the Azure Tenant ID
    - "AZURE_CLIENT_ID" the Azure Application ID
    - "AZURE_CLIENT_SECRET" a key / secret for the corresponding application
2) Client Certificate: attempt to authenticate with a Service Principal via 
    - "AZURE_TENANT_ID" the Azure Tenant ID
    - "AZURE_CLIENT_ID" the Azure Application ID
    - "AZURE_CERTIFICATE_PATH" the path to the certificate file
    - "AZURE_CERTIFICATE_PASSWORD" the password for the certificate

The Azure Environment used can be specified using the name of the Azure Environment set in "AZURE_ENVIRONMENT" var.

### Authentication
Event Hubs offers a couple different paths for authentication, shared access signatures (SAS) and Azure Active Directory (AAD)
JWT authentication. Both token types are available for use and are exposed through the `TokenProvider` interface.
```go
// TokenProvider abstracts the fetching of authentication tokens
TokenProvider interface {
    GetToken(uri string) (*Token, error)
}
```

#### SAS token provider
The SAS token provider uses the namespace of the Event Hub, the name of the "Shared access policy" key and the value of
the key to produce a token.

You can create new Shared access policies through the Azure portal as shown below.
![SAS policies in the Azure portal](./_content/sas-policy.png)

You can create a SAS token provider in a couple different ways. You can build one with a namespace, key name and key
value like this.
```go
provider, err := sas.TokenProviderWithNamespaceAndKey("mynamespace", "myKeyName", "myKeyValue")
```

Or, you can create a token provider from environment variables like this.
```go
// TokenProviderWithEnvironmentVars creates a new SAS TokenProvider from environment variables
//
// There are two sets of environment variables which can produce a SAS TokenProvider
//
// 1) Expected Environment Variables:
//   - "EVENTHUB_NAMESPACE" the namespace of the Event Hub instance
//   - "EVENTHUB_KEY_NAME" the name of the Event Hub key
//   - "EVENTHUB_KEY_VALUE" the secret for the Event Hub key named in "EVENTHUB_KEY_NAME"
//
// 2) Expected Environment Variable:
//   - "EVENTHUB_CONNECTION_STRING" connection string from the Azure portal

provider, err := sas.NewTokenProvider(sas.TokenProviderWithEnvironmentVars())
```

#### AAD JWT token provider
The AAD JWT token provider uses Azure Active Directory to authenticate the service and acquire a token (JWT) which is
used to authenticate with Event Hubs. The authenticated identity must have `Contributor` role based authorization for
the Event Hub instance. [This article](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-role-based-access-control) 
provides more information about this preview feature.

The easiest way to create a JWT token provider is via environment variables.
```go
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
provider, err := aad.NewJWTProvider(aad.JWTProviderWithEnvironmentVars())
```

You can also provide your own `adal.ServicePrincipalToken`.
```go
config := &aad.TokenProviderConfiguration{
    ResourceURI: azure.PublicCloud.ResourceManagerEndpoint,
    Env:         &azure.PublicCloud,
}

spToken, err := config.NewServicePrincipalToken()
if err != nil {
    // handle err
}
provider, err := aad.NewJWTProvider(aad.JWTProviderWithAADToken(aadToken))
```

### Send And Receive
The basics of messaging are sending and receiving messages. Here are the different ways you can do that.

#### Sending to a particular partition
By default, a Hub will send messages any of the load balanced partitions. Sometimes you want to send to only a 
particular partition. You can do this in two ways.
1) You can supply a partition key on an event
    ```go
    event := eventhub.NewEventFromString("foo")
    event.PartitionKey = "bazz"
    hub.Send(ctx, event) // send event to the partition ID to which partition key hashes
    ```
2) You can build a hub instance that will only send to one partition.
    ```go
    partitionID := "0"
    hub, err := eventhub.NewHubFromEnvironment(eventhub.HubWithPartitionedSender(partitionID))
    ```

#### Sending batches of events
Sending a batch of messages is more efficient than sending a single message.
```go
batch := &EventBatch{
            Events: []*eventhub.Event { 
                eventhub.NewEventFromString("one"),
                eventhub.NewEventFromString("two"),
            },
        }
err := client.SendBatch(ctx, batch)
```

#### Receiving
When receiving messages from an Event Hub, you always need to specify the partition you'd like to receive from. 
`Hub.Receive` is a non-blocking call, which takes a message handler func and options. Since Event Hub is just a long
log of messages, you also have to tell it where to start from. By default, a receiver will start from the beginning
of the log, but there are options to help you specify your starting offset.

The `Receive` func returns a handle to the running receiver and an error. If error is returned, the receiver was unable
to start. If error is nil, the receiver is running and can be stopped by calling `Close` on the `Hub` or the handle
returned.

- Receive messages from a partition from the beginning of the log
    ```go
    handle, err := hub.Receive(ctx, partitionID, func(ctx context.Context, event *eventhub.Event) error {
 	    // do stuff
    })
    ```
- Receive from the latest message onward
    ```go
    handle, err := hub.Receive(ctx, partitionID, handler, eventhub.ReceiveWithLatestOffset())
    ```
- Receive from a specified offset
    ```go
    handle, err := hub.Receive(ctx, partitionID, handler, eventhub.ReceiveWithStartingOffset(offset))
    ```

At some point, a receiver process is going to stop. You will likely want it to start back up at the spot that it stopped
processing messages. This is where message offsets can be used to start from where you have left off.

The `Hub` struct can be customized to use an `persist.CheckpointPersister`. By default, a `Hub` uses an in-memory
`CheckpointPersister`, but accepts anything that implements the `perist.CheckpointPersister` interface.

```go
// CheckpointPersister provides persistence for the received offset for a given namespace, hub name, consumer group, partition Id and
// offset so that if a receiver where to be interrupted, it could resume after the last consumed event.
CheckpointPersister interface {
    Write(namespace, name, consumerGroup, partitionID string, checkpoint Checkpoint) error
    Read(namespace, name, consumerGroup, partitionID string) (Checkpoint, error)
}
```

For example, you could use the persist.FilePersister to save your checkpoints to a directory.
```go
persister, err := persist.NewFilePersiter(directoryPath)
if err != nil {
	// handle err
}
hub, err := eventhub.NewHubFromEnvironment(eventhub.HubWithOffsetPersistence(persister))
```

## Event Processor Host
The Event Processor Host is a collection of features which load balances partition receivers and ensures only one 
receiver is consuming a given partition at a time. [This article](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-dotnet-standard-getstarted-receive-eph)
talks about the .NET version of the Event Processor Host. The `eph` package, once stable, will provide the equivalent
feature set.

The `eph` package is experimental.

## Examples
- [HelloWorld: Producer and Consumer](./_examples/helloworld): an example of sending and receiving messages from an
Event Hub instance.

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

See [contributing.md](./.github/contributing.md).

# License

MIT, see [LICENSE](./LICENSE).