package eventhub

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-event-hubs-go/aad"
	"github.com/Azure/azure-event-hubs-go/auth"
	"github.com/Azure/azure-event-hubs-go/sas"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"pack.ag/amqp"
)

func (suite *eventHubSuite) TestSasToken() {
	tests := map[string]func(*testing.T, Client, []string, string){
		//"TestMultiSendAndReceive":            testMultiSendAndReceive,
		"TestHubRuntimeInformation":          testHubRuntimeInformation,
		"TestHubPartitionRuntimeInformation": testHubPartitionRuntimeInformation,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hubName := randomName("goehtest", 10)
			mgmtHub, err := suite.ensureEventHub(context.Background(), hubName)
			if err != nil {
				t.Fatal(err)
			}
			defer suite.deleteEventHub(context.Background(), hubName)
			provider, err := sas.NewProviderFromEnvironment()
			if err != nil {
				t.Fatal(err)
			}
			client := suite.newClientWithProvider(t, hubName, provider)
			testFunc(t, client, *mgmtHub.PartitionIds, hubName)
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func (suite *eventHubSuite) TestPartitionedSender() {
	tests := map[string]func(*testing.T, Client, string){
		"TestSend":           testBasicSend,
		"TestSendAndReceive": testBasicSendAndReceive,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hubName := randomName("goehtest", 10)
			mgmtHub, err := suite.ensureEventHub(context.Background(), hubName)
			if err != nil {
				t.Fatal(err)
			}
			defer suite.deleteEventHub(context.Background(), hubName)
			partitionID := (*mgmtHub.PartitionIds)[0]
			client := suite.newClient(t, hubName, HubWithPartitionedSender(partitionID))

			testFunc(t, client, partitionID)
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func testBasicSend(t *testing.T, client Client, _ string) {
	err := client.Send(context.Background(), amqp.NewMessage([]byte("Hello!")))
	assert.Nil(t, err)
}

func testBasicSendAndReceive(t *testing.T, client Client, partitionID string) {
	numMessages := rand.Intn(100) + 20
	var wg sync.WaitGroup
	wg.Add(numMessages)

	messages := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		messages[i] = randomName("hello", 10)
	}

	for idx, message := range messages {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := client.Send(ctx, amqp.NewMessage([]byte(message)), SendWithMessageID(fmt.Sprintf("%d", idx)))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}

	count := 0
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := client.Receive(ctx, partitionID, func(ctx context.Context, msg *amqp.Message) error {
		assert.Equal(t, messages[count], string(msg.Data[0]))
		count++
		wg.Done()
		return nil
	}, ReceiveWithPrefetchCount(100))
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func (suite *eventHubSuite) TestMultiPartition() {
	tests := map[string]func(*testing.T, Client, []string, string){
		"TestMultiSendAndReceive": testMultiSendAndReceive,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hubName := randomName("goehtest", 10)
			mgmtHub, err := suite.ensureEventHub(context.Background(), hubName)
			if err != nil {
				t.Fatal(err)
			}
			defer suite.deleteEventHub(context.Background(), hubName)
			client := suite.newClient(t, hubName)
			testFunc(t, client, *mgmtHub.PartitionIds, hubName)
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func testMultiSendAndReceive(t *testing.T, client Client, partitionIDs []string, _ string) {
	numMessages := rand.Intn(100) + 20
	var wg sync.WaitGroup
	wg.Add(numMessages)

	messages := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		messages[i] = randomName("hello", 10)
	}

	for idx, message := range messages {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := client.Send(ctx, amqp.NewMessage([]byte(message)), SendWithMessageID(fmt.Sprintf("%d", idx)))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	for _, partitionID := range partitionIDs {
		_, err := client.Receive(ctx, partitionID, func(ctx context.Context, msg *amqp.Message) error {
			wg.Done()
			return nil
		}, ReceiveWithPrefetchCount(100))
		if err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	wg.Wait()
}

func (suite *eventHubSuite) TestHubManagement() {
	tests := map[string]func(*testing.T, Client, []string, string){
		"TestHubRuntimeInformation":          testHubRuntimeInformation,
		"TestHubPartitionRuntimeInformation": testHubPartitionRuntimeInformation,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hubName := randomName("goehtest", 10)
			mgmtHub, err := suite.ensureEventHub(context.Background(), hubName)
			if err != nil {
				t.Fatal(err)
			}
			defer suite.deleteEventHub(context.Background(), hubName)
			client := suite.newClient(t, hubName)
			testFunc(t, client, *mgmtHub.PartitionIds, hubName)
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func testHubRuntimeInformation(t *testing.T, client Client, partitionIDs []string, hubName string) {
	info, err := client.GetRuntimeInformation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	log.Debug(info.PartitionIDs)
	assert.Equal(t, len(partitionIDs), info.PartitionCount)
	assert.Equal(t, hubName, info.Path)
}

func testHubPartitionRuntimeInformation(t *testing.T, client Client, partitionIDs []string, hubName string) {
	info, err := client.GetPartitionInformation(context.Background(), partitionIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, hubName, info.HubPath)
	assert.Equal(t, partitionIDs[0], info.PartitionID)
	assert.Equal(t, "-1", info.LastEnqueuedOffset) // brand new, so should be very last
}

func TestEnvironmentalCreation(t *testing.T) {
	os.Setenv("EVENTHUB_NAME", "foo")
	_, err := NewClientFromEnvironment()
	assert.Nil(t, err)
	os.Unsetenv("EVENTHUB_NAME")
}

func BenchmarkReceive(b *testing.B) {
	suite := new(eventHubSuite)
	suite.SetupSuite()
	hubName := randomName("goehtest", 10)
	mgmtHub, err := suite.ensureEventHub(context.Background(), hubName, hubWithPartitions(8))
	if err != nil {
		b.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(b.N)

	messages := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		messages[i] = randomName("hello", 10)
	}

	provider, err := aad.NewProviderFromEnvironment()
	if err != nil {
		log.Fatal(err)
	}
	hub, err := NewClient(suite.namespace, *mgmtHub.Name, provider)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		hub.Close()
		suite.deleteEventHub(context.Background(), hubName)
	}()

	for idx, message := range messages {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := hub.Send(ctx, amqp.NewMessage([]byte(message)), SendWithMessageID(fmt.Sprintf("%d", idx)))
		cancel()
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// receive from all partition IDs
	for _, partitionID := range *mgmtHub.PartitionIds {
		_, err = hub.Receive(ctx, partitionID, func(ctx context.Context, msg *amqp.Message) error {
			wg.Done()
			return nil
		}, ReceiveWithPrefetchCount(100))
		if err != nil {
			b.Fatal(err)
		}
	}
	cancel()
	wg.Wait()
	b.StopTimer()
}

func (suite *eventHubSuite) newClient(t *testing.T, hubName string, opts ...HubOption) Client {
	provider, err := aad.NewProviderFromEnvironment(aad.JWTProviderWithEnvironment(&suite.env))
	if err != nil {
		t.Fatal(err)
	}
	return suite.newClientWithProvider(t, hubName, provider, opts...)
}

func (suite *eventHubSuite) newClientWithProvider(t *testing.T, hubName string, provider auth.TokenProvider, opts ...HubOption) Client {
	opts = append(opts, HubWithEnvironment(suite.env))
	client, err := NewClient(suite.namespace, hubName, provider, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
