package eventhub

import (
	"context"
	"fmt"
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
	tests := map[string]func(*testing.T, *Namespace, *mgmt.Model){
		"TestSend":           testBasicSend,
		"TestSendAndReceive": testBasicSendAndReceive,
	}

	ns := suite.getNamespace()

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hubName := randomName("goehtest", 10)
			mgmtHub, err := ns.EnsureEventHub(context.Background(), hubName)
			defer ns.DeleteEventHub(context.Background(), hubName)

			if err != nil {
				t.Fatal(err)
			}

			testFunc(t, ns, mgmtHub)
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func testBasicSend(t *testing.T, ns *Namespace, mgmtHub *mgmt.Model) {
	hub, err := ns.NewEventHub(*mgmtHub.Name)
	if err != nil {
		t.Fatal(err)
	}

	err = hub.Send(context.Background(), &amqp.Message{
		Data: []byte("Hello!"),
	})
	assert.Nil(t, err)
}

func testBasicSendAndReceive(t *testing.T, ns *Namespace, mgmtHub *mgmt.Model) {
	partitionID := (*mgmtHub.PartitionIds)[0]
	hub, err := ns.NewEventHub(*mgmtHub.Name, HubWithPartitionedSender(partitionID))
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()

	numMessages := rand.Intn(100) + 20
	var wg sync.WaitGroup
	wg.Add(numMessages + 1)

	messages := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		messages[i] = randomName("hello", 10)
	}

	go func() {
		for idx, message := range messages {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := hub.Send(ctx, &amqp.Message{Data: []byte(message)}, SendWithMessageID(fmt.Sprintf("%d", idx)))
			cancel()
			if err != nil {
				log.Println(idx)
				log.Fatalln(err)
			}
		}
		defer wg.Done()
	}()

	count := 0
	err = hub.Receive(partitionID, func(ctx context.Context, msg *amqp.Message) error {
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
	ns := suite.getNamespace()
	hubName := randomName("goehtest", 10)
	mgmtHub, err := ns.EnsureEventHub(context.Background(), hubName, HubWithPartitions(8))
	if err != nil {
		b.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(b.N)

	messages := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		messages[i] = randomName("hello", 10)
	}

	hub, err := ns.NewEventHub(*mgmtHub.Name)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		hub.Close()
		ns.DeleteEventHub(context.Background(), hubName)
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
