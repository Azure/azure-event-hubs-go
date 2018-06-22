package eventhub

//	MIT License
//
//	Copyright (c) Microsoft Corporation. All rights reserved.
//
//	Permission is hereby granted, free of charge, to any person obtaining a copy
//	of this software and associated documentation files (the "Software"), to deal
//	in the Software without restriction, including without limitation the rights
//	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//	copies of the Software, and to permit persons to whom the Software is
//	furnished to do so, subject to the following conditions:
//
//	The above copyright notice and this permission notice shall be included in all
//	copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//	SOFTWARE

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-amqp-common-go/aad"
	"github.com/Azure/azure-amqp-common-go/auth"
	"github.com/Azure/azure-amqp-common-go/sas"
	"github.com/Azure/azure-amqp-common-go/uuid"
	"github.com/Azure/azure-event-hubs-go/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type (
	// eventHubSuite encapsulates a end to end test of Event Hubs with build up and tear down of all EH resources
	eventHubSuite struct {
		test.BaseSuite
	}
)

var (
	defaultTimeout = 30 * time.Second
)

const (
	connStr = "Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=keyName;SharedAccessKey=secret;EntityPath=hubName"
)

func TestEH(t *testing.T) {
	suite.Run(t, new(eventHubSuite))
}

func (suite *eventHubSuite) TestNewHubWithNameAndEnvironment() {
	revert := suite.captureEnv()
	defer revert()
	os.Clearenv()
	suite.NoError(os.Setenv("EVENTHUB_CONNECTION_STRING", connStr))
	_, err := NewHubWithNamespaceNameAndEnvironment("hello", "world")
	suite.NoError(err)
}

func (suite *eventHubSuite) TestSasToken() {
	tests := map[string]func(*testing.T, *Hub, []string, string){
		"TestMultiSendAndReceive":            testMultiSendAndReceive,
		"TestHubRuntimeInformation":          testHubRuntimeInformation,
		"TestHubPartitionRuntimeInformation": testHubPartitionRuntimeInformation,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hubName := suite.RandomName("goehtest", 10)
			mgmtHub, err := suite.EnsureEventHub(context.Background(), hubName)
			if err != nil {
				t.Fatal(err)
			}
			defer suite.DeleteEventHub(context.Background(), hubName)
			provider, err := sas.NewTokenProvider(sas.TokenProviderWithEnvironmentVars())
			if err != nil {
				t.Fatal(err)
			}
			client := suite.newClientWithProvider(t, hubName, provider)
			testFunc(t, client, *mgmtHub.PartitionIds, hubName)

			closeContext, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()
			if err := client.Close(closeContext); err != nil {
				t.Fatal(err)
			}
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func (suite *eventHubSuite) TestPartitioned() {
	tests := map[string]func(*testing.T, *Hub, string){
		"TestSend":                testBasicSend,
		"TestSendAndReceive":      testBasicSendAndReceive,
		"TestBatchSendAndReceive": testBatchSendAndReceive,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hubName := suite.RandomName("goehtest", 10)
			mgmtHub, err := suite.EnsureEventHub(context.Background(), hubName)
			if err != nil {
				t.Fatal(err)
			}
			defer suite.DeleteEventHub(context.Background(), hubName)
			partitionID := (*mgmtHub.PartitionIds)[0]
			client := suite.newClient(t, hubName, HubWithPartitionedSender(partitionID))

			testFunc(t, client, partitionID)
			closeContext, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()
			if err := client.Close(closeContext); err != nil {
				t.Fatal(err)
			}
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func testBasicSend(t *testing.T, client *Hub, _ string) {
	err := client.Send(context.Background(), NewEventFromString("Hello!"))
	assert.Nil(t, err)
}

func testBatchSendAndReceive(t *testing.T, client *Hub, partitionID string) {
	messages := []string{"hello", "world", "foo", "bar", "baz", "buzz"}
	var wg sync.WaitGroup
	wg.Add(len(messages))

	events := make([]*Event, len(messages))
	for idx, msg := range messages {
		events[idx] = NewEventFromString(msg)
	}
	batch := &EventBatch{
		Events: events,
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	err := client.SendBatch(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	_, err = client.Receive(context.Background(), partitionID, func(ctx context.Context, event *Event) error {
		assert.Equal(t, messages[count], string(event.Data))
		count++
		wg.Done()
		return nil
	}, ReceiveWithPrefetchCount(100))
	if err != nil {
		t.Fatal(err)
	}

	waitUntil(t, &wg, 15*time.Second)
}

func testBasicSendAndReceive(t *testing.T, client *Hub, partitionID string) {
	numMessages := rand.Intn(100) + 20
	var wg sync.WaitGroup
	wg.Add(numMessages)

	messages := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		messages[i] = test.RandomString("hello", 10)
	}

	for idx, message := range messages {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		err := client.Send(ctx, NewEventFromString(message), SendWithMessageID(fmt.Sprintf("%d", idx)))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}

	count := 0
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	_, err := client.Receive(ctx, partitionID, func(ctx context.Context, event *Event) error {
		assert.Equal(t, messages[count], string(event.Data))
		count++
		wg.Done()
		return nil
	}, ReceiveWithPrefetchCount(100))
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, &wg, 15*time.Second)
}

func (suite *eventHubSuite) TestEpochReceivers() {
	tests := map[string]func(*testing.T, *Hub, []string, string){
		"TestEpochGreaterThenLess": testEpochGreaterThenLess,
		"TestEpochLessThenGreater": testEpochLessThenGreater,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hubName := suite.RandomName("goehtest", 10)
			mgmtHub, err := suite.EnsureEventHub(context.Background(), hubName)
			if err != nil {
				t.Fatal(err)
			}
			defer suite.DeleteEventHub(context.Background(), hubName)
			partitionID := (*mgmtHub.PartitionIds)[0]
			client := suite.newClient(t, hubName, HubWithPartitionedSender(partitionID))
			testFunc(t, client, *mgmtHub.PartitionIds, hubName)
			closeCtx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			_ = client.Close(closeCtx) // there will be an error here since the link will be forcefully detached
			defer cancel()
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func testEpochGreaterThenLess(t *testing.T, client *Hub, partitionIDs []string, _ string) {
	partitionID := partitionIDs[0]
	ctx := context.Background()
	r1, err := client.Receive(ctx, partitionID, func(c context.Context, event *Event) error { return nil }, ReceiveWithEpoch(4))
	if err != nil {
		t.Fatal(err)
	}
	r2, err := client.Receive(ctx, partitionID, func(c context.Context, event *Event) error { return nil }, ReceiveWithEpoch(1))
	if err != nil {
		t.Fatal(err)
	}

	doneCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	select {
	case <-r2.Done():
		break
	case <-doneCtx.Done():
		t.Error("r2 didn't finish in time")
	}

	if r1.Err() != nil {
		t.Error("r1 should still be running with the higher epoch")
	}

	if r2.Err() == nil {
		t.Error("r2 should have failed")
	}
}

func testEpochLessThenGreater(t *testing.T, client *Hub, partitionIDs []string, _ string) {
	partitionID := partitionIDs[0]
	ctx := context.Background()
	r1, err := client.Receive(ctx, partitionID, func(c context.Context, event *Event) error { return nil }, ReceiveWithEpoch(1))
	if err != nil {
		t.Fatal(err)
	}

	r2, err := client.Receive(ctx, partitionID, func(c context.Context, event *Event) error { return nil }, ReceiveWithEpoch(4))
	if err != nil {
		t.Fatal(err)
	}

	doneCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	select {
	case <-r1.Done():
		break
	case <-doneCtx.Done():
		t.Error("r1 didn't finish in time")
	}

	if r1.Err() == nil {
		t.Error("r1 should have died with error since it has a lower epoch value")
	}

	if r2.Err() != nil {
		t.Error("r2 should not have an error and should still be processing")
	}
}

func (suite *eventHubSuite) TestMultiPartition() {
	tests := map[string]func(*testing.T, *Hub, []string, string){
		"TestMultiSendAndReceive":            testMultiSendAndReceive,
		"TestSendWithPartitionKeyAndReceive": testSendWithPartitionKeyAndReceive,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hubName := suite.RandomName("goehtest", 10)
			mgmtHub, err := suite.EnsureEventHub(context.Background(), hubName)
			if err != nil {
				t.Fatal(err)
			}
			defer suite.DeleteEventHub(context.Background(), hubName)
			client := suite.newClient(t, hubName)
			testFunc(t, client, *mgmtHub.PartitionIds, hubName)
			closeContext, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()
			if err := client.Close(closeContext); err != nil {
				t.Fatal(err)
			}
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func testMultiSendAndReceive(t *testing.T, client *Hub, partitionIDs []string, _ string) {
	numMessages := rand.Intn(100) + 20
	var wg sync.WaitGroup
	wg.Add(numMessages)

	messages := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		messages[i] = test.RandomString("hello", 10)
	}

	for idx, message := range messages {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		err := client.Send(ctx, NewEventFromString(message), SendWithMessageID(fmt.Sprintf("%d", idx)))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	for _, partitionID := range partitionIDs {
		_, err := client.Receive(ctx, partitionID, func(ctx context.Context, event *Event) error {
			wg.Done()
			return nil
		}, ReceiveWithPrefetchCount(100))
		if err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, &wg, 15*time.Second)
}

func testSendWithPartitionKeyAndReceive(t *testing.T, client *Hub, partitionIDs []string, _ string) {
	numMessages := rand.Intn(100) + 20
	var wg sync.WaitGroup
	wg.Add(numMessages)

	sentEvents := make(map[string]*Event)
	for i := 0; i < numMessages; i++ {
		content := test.RandomString("hello", 10)
		event := NewEventFromString(content)
		event.PartitionKey = &content
		id, err := uuid.NewV4()
		if !assert.NoError(t, err) {
			assert.FailNow(t, "error generating uuid")
		}
		event.ID = id.String()
		sentEvents[event.ID] = event
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	for _, event := range sentEvents {
		if !assert.NoError(t, client.Send(ctx, event)) {
			assert.FailNow(t, "failed to send message to hub")
		}
	}

	received := make(map[string][]*Event)
	for _, pID := range partitionIDs {
		received[pID] = []*Event{}
	}
	for _, partitionID := range partitionIDs {
		_, err := client.Receive(ctx, partitionID, func(ctx context.Context, event *Event) error {
			defer wg.Done()
			received[partitionID] = append(received[partitionID], event)
			return nil
		}, ReceiveWithPrefetchCount(100))
		if !assert.NoError(t, err) {
			assert.FailNow(t, "failed to receive from partition")
		}
	}

	// wait for events to arrive
	end, _ := ctx.Deadline()
	if waitUntil(t, &wg, time.Until(end)) {
		// collect all of the partitioned events
		receivedEventsByID := make(map[string]*Event)
		for _, pID := range partitionIDs {
			for _, event := range received[pID] {
				receivedEventsByID[event.ID] = event
			}
		}

		// verify the sent events have the same partition keys as the received events
		for key, event := range sentEvents {
			assert.Equal(t, event.PartitionKey, receivedEventsByID[key].PartitionKey)
		}
	}
}

func (suite *eventHubSuite) TestHubManagement() {
	tests := map[string]func(*testing.T, *Hub, []string, string){
		"TestHubRuntimeInformation":          testHubRuntimeInformation,
		"TestHubPartitionRuntimeInformation": testHubPartitionRuntimeInformation,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hubName := suite.RandomName("goehtest", 10)
			mgmtHub, err := suite.EnsureEventHub(context.Background(), hubName)
			if err != nil {
				t.Fatal(err)
			}
			defer suite.DeleteEventHub(context.Background(), hubName)
			client := suite.newClient(t, hubName)
			testFunc(t, client, *mgmtHub.PartitionIds, hubName)
			closeContext, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()
			if err := client.Close(closeContext); err != nil {
				t.Fatal(err)
			}
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func testHubRuntimeInformation(t *testing.T, client *Hub, partitionIDs []string, hubName string) {
	info, err := client.GetRuntimeInformation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, len(partitionIDs), info.PartitionCount)
	assert.Equal(t, hubName, info.Path)
}

func testHubPartitionRuntimeInformation(t *testing.T, client *Hub, partitionIDs []string, hubName string) {
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
	_, err := NewHubFromEnvironment()
	assert.Nil(t, err)
	os.Unsetenv("EVENTHUB_NAME")
}

func BenchmarkReceive(b *testing.B) {
	testSuite := new(eventHubSuite)
	testSuite.SetupSuite()
	hubName := testSuite.RandomName("goehtest", 10)
	mgmtHub, err := testSuite.EnsureEventHub(context.Background(), hubName, test.HubWithPartitions(8))
	if err != nil {
		b.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(b.N)

	messages := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		messages[i] = testSuite.RandomName("hello", 10)
	}

	provider, err := aad.NewJWTProvider(aad.JWTProviderWithEnvironmentVars())
	if err != nil {
		b.Fatal(err)
	}
	hub, err := NewHub(testSuite.Namespace, *mgmtHub.Name, provider)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		closeContext, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		hub.Close(closeContext)
		cancel()
		testSuite.DeleteEventHub(context.Background(), hubName)
	}()

	for idx, message := range messages {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		err := hub.Send(ctx, NewEventFromString(message), SendWithMessageID(fmt.Sprintf("%d", idx)))
		cancel()
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	// receive from all partition IDs
	for _, partitionID := range *mgmtHub.PartitionIds {
		_, err = hub.Receive(ctx, partitionID, func(ctx context.Context, event *Event) error {
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

func (suite *eventHubSuite) newClient(t *testing.T, hubName string, opts ...HubOption) *Hub {
	provider, err := aad.NewJWTProvider(aad.JWTProviderWithEnvironmentVars(), aad.JWTProviderWithAzureEnvironment(&suite.Env))
	if err != nil {
		t.Fatal(err)
	}
	return suite.newClientWithProvider(t, hubName, provider, opts...)
}

func (suite *eventHubSuite) newClientWithProvider(t *testing.T, hubName string, provider auth.TokenProvider, opts ...HubOption) *Hub {
	opts = append(opts, HubWithEnvironment(suite.Env))
	client, err := NewHub(suite.Namespace, hubName, provider, opts...)
	suite.NoError(err)
	return client
}

func waitUntil(t *testing.T, wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(d):
		assert.Fail(t, "took longer than "+fmtDuration(d))
		return false
	}
}

func fmtDuration(d time.Duration) string {
	d = d.Round(time.Second) / time.Second
	return fmt.Sprintf("%d seconds", d)
}

func restoreEnv(capture map[string]string) error {
	os.Clearenv()
	for key, value := range capture {
		err := os.Setenv(key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (suite *eventHubSuite) captureEnv() func() {
	capture := make(map[string]string)
	for _, pair := range os.Environ() {
		keyValue := strings.Split(pair, "=")
		capture[keyValue[0]] = strings.Join(keyValue[1:], "=")
	}
	return func() {
		suite.NoError(restoreEnv(capture))
	}
}
