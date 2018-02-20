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
	"github.com/Azure/azure-event-hubs-go/sas"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"pack.ag/amqp"
)

func (suite *eventHubSuite) TestSasToken() {
	tests := map[string]func(*testing.T, Client, string){
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
			provider, err := sas.NewProviderFromEnvironment()
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(suite.namespace, hubName, provider, HubWithPartitionedSender(partitionID))
			if err != nil {
				t.Fatal(err)
			}

			testFunc(t, client, partitionID)
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
			provider, err := aad.NewProviderFromEnvironment()
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(suite.namespace, hubName, provider, HubWithPartitionedSender(partitionID))
			if err != nil {
				t.Fatal(err)
			}

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
	err := client.Receive(partitionID, func(ctx context.Context, msg *amqp.Message) error {
		assert.Equal(t, messages[count], string(msg.Data[0]))
		count++
		wg.Done()
		return nil
	}, ReceiveWithPrefetchCount(100))
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func (suite *eventHubSuite) TestMultiPartition() {
	tests := map[string]func(*testing.T, Client, []string){
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
			provider, err := aad.NewProviderFromEnvironment()
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(suite.namespace, hubName, provider)
			if err != nil {
				t.Fatal(err)
			}

			testFunc(t, client, *mgmtHub.PartitionIds)
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func testMultiSendAndReceive(t *testing.T, client Client, partitionIDs []string) {
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

	for _, partitionID := range partitionIDs {
		err := client.Receive(partitionID, func(ctx context.Context, msg *amqp.Message) error {
			wg.Done()
			return nil
		}, ReceiveWithPrefetchCount(100))
		if err != nil {
			t.Fatal(err)
		}
	}
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
			provider, err := aad.NewProviderFromEnvironment()
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(suite.namespace, hubName, provider)
			if err != nil {
				t.Fatal(err)
			}

			testFunc(t, client, *mgmtHub.PartitionIds, *mgmtHub.Name)
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

	// receive from all partition IDs
	for _, partitionID := range *mgmtHub.PartitionIds {
		err = hub.Receive(partitionID, func(ctx context.Context, msg *amqp.Message) error {
			wg.Done()
			return nil
		}, ReceiveWithPrefetchCount(100))
		if err != nil {
			b.Fatal(err)
		}
	}

	wg.Wait()
	b.StopTimer()
}
