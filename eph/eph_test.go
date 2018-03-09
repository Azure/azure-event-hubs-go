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
	testSuite "github.com/stretchr/testify/suite"
)

type (
	// eventHubSuite encapsulates a end to end test of Event Hubs with build up and tear down of all EH resources
	suite struct {
		test.BaseSuite
	}
)

func TestEventProcessorHost(t *testing.T) {
	testSuite.Run(t, new(suite))
}

func (s *suite) TestSingle() {
	hubName := test.RandomName("goehtest", 10)
	_, err := s.EnsureEventHub(context.Background(), hubName)
	if err != nil {
		s.T().Fatal(err)
	}
	defer s.DeleteEventHub(context.Background(), hubName)

	provider, err := aad.NewProviderFromEnvironment()
	if err != nil {
		s.T().Fatal(err)
	}

	leaseBuilder := func(processor *EventProcessorHost, defaultLeaseDuration time.Duration) Leaser {
		return newMemoryLeaser(processor.name, 2*time.Second)
	}

	checkpointBuilder := func(processor *EventProcessorHost) Checkpointer {
		return newMemoryCheckpointer(processor)
	}

	processor, err := New(s.Namespace, hubName, provider, leaseBuilder, checkpointBuilder)
	if err != nil {
		s.T().Error(err)
	}

	client := s.newClientWithProvider(s.T(), hubName, provider)
	defer client.Close()

	messages := []string{"bin", "bazz", "foo", "bar", "buzz"}
	var wg sync.WaitGroup
	wg.Add(len(messages))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	for _, msg := range messages {
		client.Send(ctx, eventhub.NewEventFromString(msg))
	}
	cancel()

	processor.Receive(func(c context.Context, event *eventhub.Event) error {
		wg.Done()
		return nil
	})

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	processor.StartNonBlocking(ctx)
	cancel()
	defer processor.Close()

	waitUntil(s.T(), &wg, 30*time.Second)
}

func (s *suite) newClient(t *testing.T, hubName string, opts ...eventhub.HubOption) eventhub.Client {
	provider, err := aad.NewProviderFromEnvironment(aad.JWTProviderWithEnvironment(&s.Env))
	if err != nil {
		t.Fatal(err)
	}
	return s.newClientWithProvider(t, hubName, provider, opts...)
}

func (s *suite) newClientWithProvider(t *testing.T, hubName string, provider auth.TokenProvider, opts ...eventhub.HubOption) eventhub.Client {
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
