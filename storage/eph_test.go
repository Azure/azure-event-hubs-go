package storage

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-event-hubs-go"
	"github.com/Azure/azure-event-hubs-go/aad"
	"github.com/Azure/azure-event-hubs-go/auth"
	"github.com/Azure/azure-event-hubs-go/eph"
	"github.com/Azure/azure-event-hubs-go/internal/test"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
)

func (ts *testSuite) TestSingle() {
	randomName := strings.ToLower(test.RandomName("gostoreph", 4))
	hub, delHub := ts.ensureRandomHubByName(randomName)
	delContainer := ts.newTestContainerByName(randomName)
	defer delContainer()

	processor, err := ts.newStorageBackedEPH(*hub.Name, randomName)
	if err != nil {
		ts.T().Fatal(err)
	}
	defer func() {
		processor.Close()
		delHub()
	}()

	messages, err := ts.sendMessages(*hub.Name, 10)
	if err != nil {
		ts.T().Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(len(messages))

	processor.Receive(func(c context.Context, event *eventhub.Event) error {
		wg.Done()
		return nil
	})

	processor.StartNonBlocking(context.Background())
	waitUntil(ts.T(), &wg, 30*time.Second)
}

func (ts *testSuite) TestMultiple() {
	randomName := strings.ToLower(test.RandomName("gostoreph", 4))
	hub, delHub := ts.ensureRandomHubByName(randomName)
	delContainer := ts.newTestContainerByName(randomName)
	defer delContainer()

	cred, err := NewAADSASCredential(ts.SubscriptionID, test.ResourceGroupName, ts.AccountName, randomName, AADSASCredentialWithEnvironmentVars())
	if err != nil {
		ts.T().Fatal(err)
	}

	numPartitions := len(*hub.PartitionIds)
	processors := make([]*eph.EventProcessorHost, numPartitions)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < numPartitions; i++ {
		leaserCheckpointer, err := NewStorageLeaserCheckpointer(cred, ts.AccountName, randomName, ts.Env)
		if err != nil {
			ts.T().Fatal(err)
		}

		processor, err := ts.newStorageBackedEPHOptions(*hub.Name, leaserCheckpointer, leaserCheckpointer)
		if err != nil {
			ts.T().Fatal(err)
		}

		processors[i] = processor
		processor.StartNonBlocking(ctx)
	}

	defer func() {
		for i := 0; i < numPartitions; i++ {
			processors[i].Close()
		}
		delHub()
	}()

	count := 0
	var partitionMap map[string]bool
	for {
		<-time.After(2 * time.Second)
		count++
		if count > 60 {
			break
		}

		partitionMap = newPartitionMap(*hub.PartitionIds)
		for i := 0; i < numPartitions; i++ {
			partitions := processors[i].PartitionIDsBeingProcessed()
			if len(partitions) == 1 {
				partitionMap[partitions[0]] = true
			}
		}
		log.Println(partitionMap)
		if allTrue(partitionMap) {
			break
		}
	}
	if !allTrue(partitionMap) {
		ts.T().Error("never balanced work within allotted time")
		return
	}

	processors[numPartitions-1].Close() // close the last partition
	count = 0
	for {
		<-time.After(2 * time.Second)
		count++
		if count > 60 {
			break
		}

		partitionMap = newPartitionMap(*hub.PartitionIds)
		for i := 0; i < numPartitions-1; i++ {
			partitions := processors[i].PartitionIDsBeingProcessed()
			for _, partition := range partitions {
				partitionMap[partition] = true
			}
		}
		log.Println(partitionMap)
		if allTrue(partitionMap) {
			break
		}
	}
	if !allTrue(partitionMap) {
		ts.T().Error("didn't balance after closing a processor")
	}
}

func (ts *testSuite) newTestContainerByName(containerName string) func() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cred, err := NewAADSASCredential(ts.SubscriptionID, test.ResourceGroupName, ts.AccountName, containerName, AADSASCredentialWithEnvironmentVars())
	if err != nil {
		ts.T().Fatal(err)
	}

	pipeline := azblob.NewPipeline(cred, azblob.PipelineOptions{})
	fooURL, err := url.Parse("https://" + ts.AccountName + ".blob." + ts.Env.StorageEndpointSuffix + "/" + containerName)
	if err != nil {
		ts.T().Error(err)
	}

	containerURL := azblob.NewContainerURL(*fooURL, pipeline)
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		ts.T().Error(err)
	}

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		containerURL.Delete(ctx, azblob.ContainerAccessConditions{})
	}
}

func (ts *testSuite) newTestContainer(prefix string, length int) (string, func()) {
	name := strings.ToLower(test.RandomName(prefix, length))
	return name, ts.newTestContainerByName(name)
}

func (ts *testSuite) sendMessages(hubName string, length int) ([]string, error) {
	client := ts.newClient(ts.T(), hubName)
	defer client.Close()

	messages := make([]string, length)
	for i := 0; i < length; i++ {
		messages[i] = test.RandomName("message", 5)
	}

	events := make([]*eventhub.Event, length)
	for idx, msg := range messages {
		events[idx] = eventhub.NewEventFromString(msg)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client.SendBatch(ctx, eventhub.NewEventBatch(events))

	return messages, ctx.Err()
}

func (ts *testSuite) newStorageBackedEPH(hubName, containerName string) (*eph.EventProcessorHost, error) {
	cred, err := NewAADSASCredential(ts.SubscriptionID, test.ResourceGroupName, ts.AccountName, containerName, AADSASCredentialWithEnvironmentVars())
	if err != nil {
		ts.T().Fatal(err)
	}
	leaserCheckpointer, err := NewStorageLeaserCheckpointer(cred, ts.AccountName, containerName, ts.Env)
	if err != nil {
		ts.T().Fatal(err)
	}

	return ts.newStorageBackedEPHOptions(hubName, leaserCheckpointer, leaserCheckpointer)
}

func (ts *testSuite) newStorageBackedEPHOptions(hubName string, leaser eph.Leaser, checkpointer eph.Checkpointer) (*eph.EventProcessorHost, error) {
	provider, err := aad.NewJWTProvider(aad.JWTProviderWithEnvironmentVars())
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	processor, err := eph.New(ctx, ts.Namespace, hubName, provider, leaser, checkpointer)
	if err != nil {
		return nil, err
	}

	return processor, nil
}

func (ts *testSuite) newClient(t *testing.T, hubName string, opts ...eventhub.HubOption) *eventhub.Hub {
	provider, err := aad.NewJWTProvider(aad.JWTProviderWithEnvironmentVars(), aad.JWTProviderWithAzureEnvironment(&ts.Env))
	if err != nil {
		t.Fatal(err)
	}
	return ts.newClientWithProvider(t, hubName, provider, opts...)
}

func (ts *testSuite) newClientWithProvider(t *testing.T, hubName string, provider auth.TokenProvider, opts ...eventhub.HubOption) *eventhub.Hub {
	opts = append(opts, eventhub.HubWithEnvironment(ts.Env))
	client, err := eventhub.NewHub(ts.Namespace, hubName, provider, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func waitUntil(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(d):
		t.Error("took longer than " + fmtDuration(d))
	}
}

func fmtDuration(d time.Duration) string {
	d = d.Round(time.Second) / time.Second
	return fmt.Sprintf("%d seconds", d)
}

func allTrue(partitionMap map[string]bool) bool {
	for key := range partitionMap {
		if !partitionMap[key] {
			return false
		}
	}
	return true
}

func newPartitionMap(partitionIDs []string) map[string]bool {
	partitionMap := make(map[string]bool)
	for _, partition := range partitionIDs {
		partitionMap[partition] = false
	}
	return partitionMap
}
