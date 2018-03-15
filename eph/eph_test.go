package eph

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-event-hubs-go"
	"github.com/Azure/azure-event-hubs-go/aad"
	"github.com/Azure/azure-event-hubs-go/auth"
	"github.com/Azure/azure-event-hubs-go/internal/test"
	mgmt "github.com/Azure/azure-sdk-for-go/services/eventhub/mgmt/2017-04-01/eventhub"
	"github.com/stretchr/testify/suite"
)

type (
	// eventHubSuite encapsulates a end to end test of Event Hubs with build up and tear down of all EH resources
	testSuite struct {
		test.BaseSuite
	}
)

func TestEventProcessorHost(t *testing.T) {
	suite.Run(t, new(testSuite))
}

func (s *testSuite) TestSingle() {
	hub, del := s.ensureRandomHub("goEPH", 10)
	defer del()

	processor, err := s.newInMemoryEPH(*hub.Name)
	if err != nil {
		s.T().Fatal(err)
	}

	messages, err := s.sendMessages(*hub.Name, 10)
	if err != nil {
		s.T().Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(len(messages))

	processor.Receive(func(c context.Context, event *eventhub.Event) error {
		wg.Done()
		return nil
	})

	processor.StartNonBlocking(context.Background())
	defer processor.Close()

	waitUntil(s.T(), &wg, 30*time.Second)
}

func (s *testSuite) TestMultiple() {
	hub, del := s.ensureRandomHub("goEPH", 10)
	defer del()

	numPartitions := len(*hub.PartitionIds)
	leaser := newMemoryLeaser(11 * time.Second)
	checkpointer := new(memoryCheckpointer)
	processors := make([]*EventProcessorHost, numPartitions)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < numPartitions; i++ {
		processor, err := s.newInMemoryEPHWithOptions(*hub.Name, leaser, checkpointer)
		if err != nil {
			s.T().Fatal(err)
		}
		processors[i] = processor
		processor.StartNonBlocking(ctx)
	}

	defer func() {
		for i := 0; i < numPartitions; i++ {
			processors[i].Close()
		}
	}()

	count := 0
	allBalanced := false
	for {
		<-time.After(1 * time.Second)
		count++
		if count > 30 {
			break
		}

		allBalanced = true
		for i := 0; i < numPartitions; i++ {
			numReceivers := len(processors[i].scheduler.receivers)
			if numReceivers != 1 {
				allBalanced = false
			}
		}
		if allBalanced {
			break
		}
	}
	if !allBalanced {
		s.T().Error("never balanced work within allotted time")
		return
	}

	processors[numPartitions-1].Close() // close the last partition
	allBalanced = false
	count = 0
	for {
		<-time.After(1 * time.Second)
		count++
		if count > 20 {
			break
		}

		partitionsProcessing := 0
		for i := 0; i < numPartitions-1; i++ {
			numReceivers := len(processors[i].scheduler.receivers)
			partitionsProcessing += numReceivers
		}
		allBalanced = partitionsProcessing == numPartitions
		if allBalanced {
			break
		}
	}
	if !allBalanced {
		s.T().Error("didn't balance after closing a processor")
	}
}

func (s *testSuite) sendMessages(hubName string, length int) ([]string, error) {
	client := s.newClient(s.T(), hubName)
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

func (s *testSuite) ensureRandomHub(prefix string, length int) (*mgmt.Model, func()) {
	hubName := test.RandomName(prefix, length)
	hub, err := s.EnsureEventHub(context.Background(), hubName)
	if err != nil {
		s.T().Fatal(err)
	}

	return hub, func() {
		s.DeleteEventHub(context.Background(), hubName)
	}
}

func (s *testSuite) newInMemoryEPH(hubName string) (*EventProcessorHost, error) {
	leaser := newMemoryLeaser(2 * time.Second)
	checkpointer := new(memoryCheckpointer)
	return s.newInMemoryEPHWithOptions(hubName, leaser, checkpointer)
}

func (s *testSuite) newInMemoryEPHWithOptions(hubName string, leaser Leaser, checkpointer Checkpointer) (*EventProcessorHost, error) {
	provider, err := aad.NewJWTProvider(aad.JWTProviderWithEnvironmentVars())
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	processor, err := New(ctx, s.Namespace, hubName, provider, leaser, checkpointer)
	if err != nil {
		return nil, err
	}

	return processor, nil
}

func (s *testSuite) newClient(t *testing.T, hubName string, opts ...eventhub.HubOption) eventhub.Client {
	provider, err := aad.NewJWTProvider(aad.JWTProviderWithEnvironmentVars(), aad.JWTProviderWithAzureEnvironment(&s.Env))
	if err != nil {
		t.Fatal(err)
	}
	return s.newClientWithProvider(t, hubName, provider, opts...)
}

func (s *testSuite) newClientWithProvider(t *testing.T, hubName string, provider auth.TokenProvider, opts ...eventhub.HubOption) eventhub.Client {
	opts = append(opts, eventhub.HubWithEnvironment(s.Env))
	client, err := eventhub.NewClient(s.Namespace, hubName, provider, opts...)
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
