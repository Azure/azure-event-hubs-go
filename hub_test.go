package eventhub

import (
	"context"
	"fmt"
	"github.com/Azure/azure-event-hubs-go/aad"
	mgmt "github.com/Azure/azure-sdk-for-go/services/eventhub/mgmt/2017-04-01/eventhub"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"pack.ag/amqp"
	"sync"
	"testing"
	"time"
)

func (suite *eventHubSuite) TestBasicOperations() {
	tests := map[string]func(*testing.T, Client, *mgmt.Model){
		"TestSend":           testBasicSend,
		"TestSendAndReceive": testBasicSendAndReceive,
	}

	token, err := suite.getEventHubsTokenProvider()
	if err != nil {
		log.Fatal(err)
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hubName := randomName("goehtest", 10)
			mgmtHub, err := suite.ensureEventHub(context.Background(), hubName)
			if err != nil {
				t.Fatal(err)
			}
			defer suite.deleteEventHub(context.Background(), hubName)

			provider := aad.NewProvider(token)
			client, err := NewClient(suite.namespace, hubName, provider)
			if err != nil {
				t.Fatal(err)
			}

			testFunc(t, client, mgmtHub)
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func testBasicSend(t *testing.T, client Client, _ *mgmt.Model) {
	err := client.Send(context.Background(), &amqp.Message{
		Data: []byte("Hello!"),
	})
	assert.Nil(t, err)
}

func testBasicSendAndReceive(t *testing.T, client Client, mgmtHub *mgmt.Model) {
	partitionID := (*mgmtHub.PartitionIds)[0]

	numMessages := rand.Intn(100) + 20
	var wg sync.WaitGroup
	wg.Add(numMessages)

	messages := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		messages[i] = randomName("hello", 10)
	}

	for idx, message := range messages {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := client.Send(ctx, &amqp.Message{Data: []byte(message)}, SendWithMessageID(fmt.Sprintf("%d", idx)))
		cancel()
		if err != nil {
			log.Fatalln(err)
		}
	}

	count := 0
	err := client.Receive(partitionID, func(ctx context.Context, msg *amqp.Message) error {
		assert.Equal(t, messages[count], string(msg.Data))
		count++
		wg.Done()
		return nil
	}, ReceiveWithPrefetchCount(100))
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

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

	token, err := suite.getEventHubsTokenProvider()
	provider := aad.NewProvider(token)
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
		err := hub.Send(ctx, &amqp.Message{Data: []byte(message)}, SendWithMessageID(fmt.Sprintf("%d", idx)))
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
