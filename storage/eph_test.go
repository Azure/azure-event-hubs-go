package storage

import (
	"context"
	"fmt"
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
	"github.com/stretchr/testify/suite"
)

func TestEventProcessorHost(t *testing.T) {
	suite.Run(t, new(testSuite))
}

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

//func (ts *testSuite) TestMultiple() {
//	hub, del := s.ensureRandomHub("goEPH", 10)
//	defer del()
//
//	numPartitions := len(*hub.PartitionIds)
//	leaser := newMemoryLeaser(11 * time.Second)
//	checkpointer := new(memoryCheckpointer)
//	processors := make([]*EventProcessorHost, numPartitions)
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//	for i := 0; i < numPartitions; i++ {
//		processor, err := s.newInMemoryEPHWithOptions(*hub.Name, leaser, checkpointer)
//		if err != nil {
//			s.T().Fatal(err)
//		}
//		processors[i] = processor
//		processor.StartNonBlocking(ctx)
//	}
//
//	defer func() {
//		for i := 0; i < numPartitions; i++ {
//			processors[i].Close()
//		}
//	}()
//
//	count := 0
//	allBalanced := false
//	for {
//		<-time.After(1 * time.Second)
//		count++
//		if count > 30 {
//			break
//		}
//
//		allBalanced = true
//		for i := 0; i < numPartitions; i++ {
//			numReceivers := len(processors[i].scheduler.receivers)
//			if numReceivers != 1 {
//				allBalanced = false
//			}
//		}
//		if allBalanced {
//			break
//		}
//	}
//	if !allBalanced {
//		s.T().Error("never balanced work within allotted time")
//		return
//	}
//
//	processors[numPartitions-1].Close() // close the last partition
//	allBalanced = false
//	count = 0
//	for {
//		<-time.After(1 * time.Second)
//		count++
//		if count > 20 {
//			break
//		}
//
//		partitionsProcessing := 0
//		for i := 0; i < numPartitions-1; i++ {
//			numReceivers := len(processors[i].scheduler.receivers)
//			partitionsProcessing += numReceivers
//		}
//		allBalanced = partitionsProcessing == numPartitions
//		if allBalanced {
//			break
//		}
//	}
//	if !allBalanced {
//		s.T().Error("didn't balance after closing a processor")
//	}
//}

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

func (ts *testSuite) newClient(t *testing.T, hubName string, opts ...eventhub.HubOption) eventhub.Client {
	provider, err := aad.NewJWTProvider(aad.JWTProviderWithEnvironmentVars(), aad.JWTProviderWithAzureEnvironment(&ts.Env))
	if err != nil {
		t.Fatal(err)
	}
	return ts.newClientWithProvider(t, hubName, provider, opts...)
}

func (ts *testSuite) newClientWithProvider(t *testing.T, hubName string, provider auth.TokenProvider, opts ...eventhub.HubOption) eventhub.Client {
	opts = append(opts, eventhub.HubWithEnvironment(ts.Env))
	client, err := eventhub.NewClient(ts.Namespace, hubName, provider, opts...)
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
