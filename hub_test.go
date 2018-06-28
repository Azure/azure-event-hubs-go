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

// Delete all of the hubs under a given namespace
// az eventhubs eventhub delete -g ehtest --namespace-name {ns} --ids $(az eventhubs eventhub list -g ehtest --namespace-name {ns} --query "[].id" -o tsv)

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
	"github.com/opentracing/opentracing-go"
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
	tests := map[string]func(context.Context, *testing.T, *Hub, []string, string){
		"TestMultiSendAndReceive":            testMultiSendAndReceive,
		"TestHubRuntimeInformation":          testHubRuntimeInformation,
		"TestHubPartitionRuntimeInformation": testHubPartitionRuntimeInformation,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			provider, err := sas.NewTokenProvider(sas.TokenProviderWithEnvironmentVars())
			if !suite.NoError(err) {
				suite.FailNow("unable to build SAS token from environment vars")
			}

			hub, cleanup := suite.RandomHub()
			defer cleanup()
			client, closer := suite.newClientWithProvider(t, *hub.Name, provider)
			defer closer()
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()
			testFunc(ctx, t, client, *hub.PartitionIds, *hub.Name)
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func (suite *eventHubSuite) TestPartitioned() {
	tests := map[string]func(context.Context, *testing.T, *Hub, string){
		"TestSend":                testBasicSend,
		"TestSendAndReceive":      testBasicSendAndReceive,
		"TestBatchSendAndReceive": testBatchSendAndReceive,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hub, cleanup := suite.RandomHub()
			defer cleanup()
			partitionID := (*hub.PartitionIds)[0]
			client, closer := suite.newClient(t, *hub.Name, HubWithPartitionedSender(partitionID))
			defer closer()
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()
			testFunc(ctx, t, client, partitionID)
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func testBasicSend(ctx context.Context, t *testing.T, client *Hub, _ string) {
	err := client.Send(ctx, NewEventFromString("Hello!"))
	assert.Nil(t, err)
}

func testBatchSendAndReceive(ctx context.Context, t *testing.T, client *Hub, partitionID string) {
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

	if assert.NoError(t, client.SendBatch(ctx, batch)) {
		count := 0
		_, err := client.Receive(context.Background(), partitionID, func(ctx context.Context, event *Event) error {
			assert.Equal(t, messages[count], string(event.Data))
			count++
			wg.Done()
			return nil
		}, ReceiveWithPrefetchCount(100))
		if !assert.NoError(t, err) {
			end, _ := ctx.Deadline()
			waitUntil(t, &wg, time.Until(end))
		}
	}
}

func testBasicSendAndReceive(ctx context.Context, t *testing.T, client *Hub, partitionID string) {
	numMessages := rand.Intn(100) + 20
	var wg sync.WaitGroup
	wg.Add(numMessages)

	messages := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		messages[i] = test.RandomString("hello", 10)
	}

	for idx, message := range messages {
		if !assert.NoError(t, client.Send(ctx, NewEventFromString(message), SendWithMessageID(fmt.Sprintf("%d", idx)))) {
			assert.FailNow(t, "unable to send event")
		}
	}

	count := 0
	_, err := client.Receive(ctx, partitionID, func(ctx context.Context, event *Event) error {
		assert.Equal(t, messages[count], string(event.Data))
		count++
		wg.Done()
		return nil
	}, ReceiveWithPrefetchCount(100))
	if !assert.NoError(t, err) {
		end, _ := ctx.Deadline()
		waitUntil(t, &wg, time.Until(end))
	}
}

func (suite *eventHubSuite) TestEpochReceivers() {
	tests := map[string]func(context.Context, *testing.T, *Hub, []string, string){
		"TestEpochGreaterThenLess": testEpochGreaterThenLess,
		"TestEpochLessThenGreater": testEpochLessThenGreater,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hub, cleanup := suite.RandomHub()
			defer cleanup()
			partitionID := (*hub.PartitionIds)[0]
			client, closer := suite.newClient(t, *hub.Name, HubWithPartitionedSender(partitionID))
			defer closer()
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()
			testFunc(ctx, t, client, *hub.PartitionIds, *hub.Name)
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func testEpochGreaterThenLess(ctx context.Context, t *testing.T, client *Hub, partitionIDs []string, _ string) {
	partitionID := partitionIDs[0]
	r1, err := client.Receive(ctx, partitionID, func(c context.Context, event *Event) error { return nil }, ReceiveWithEpoch(4))
	if !assert.NoError(t, err) {
		assert.FailNow(t, "error receiving with epoch of 4")
	}

	r2, err := client.Receive(ctx, partitionID, func(c context.Context, event *Event) error { return nil }, ReceiveWithEpoch(1))
	if !assert.NoError(t, err) {
		assert.FailNow(t, "error receiving with epoch of 1")
	}

	select {
	case <-r2.Done():
		break
	case <-ctx.Done():
		assert.FailNow(t, "r2 didn't finish in time")
	}

	assert.NoError(t, r1.Err(), "r1 should still be running with the higher epoch")
	assert.Error(t, r2.Err(), "r2 should have failed")
}

func testEpochLessThenGreater(ctx context.Context, t *testing.T, client *Hub, partitionIDs []string, _ string) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "testEpochLessThenGreater")
	span.Finish()
	partitionID := partitionIDs[0]
	r1, err := client.Receive(ctx, partitionID, func(c context.Context, event *Event) error { return nil }, ReceiveWithEpoch(1))
	if !assert.NoError(t, err) {
		assert.FailNow(t, "error receiving with epoch of 1")
	}

	r2, err := client.Receive(ctx, partitionID, func(c context.Context, event *Event) error { return nil }, ReceiveWithEpoch(4))
	if !assert.NoError(t, err) {
		assert.FailNow(t, "error receiving with epoch of 4")
	}

	select {
	case <-r1.Done():
		break
	case <-ctx.Done():
		assert.FailNow(t, "r1 didn't finish in time")
	}

	assert.Error(t, r1.Err(), "r1 should have died with error since it has a lower epoch value")
	assert.NoError(t, r2.Err(), "r2 should not have an error and should still be processing")
}

func (suite *eventHubSuite) TestMultiPartition() {
	tests := map[string]func(context.Context, *testing.T, *Hub, []string, string){
		"TestMultiSendAndReceive":            testMultiSendAndReceive,
		"TestSendWithPartitionKeyAndReceive": testSendWithPartitionKeyAndReceive,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hub, cleanup := suite.RandomHub()
			defer cleanup()
			client, closer := suite.newClient(t, *hub.Name)
			defer closer()
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()
			testFunc(ctx, t, client, *hub.PartitionIds, *hub.Name)
		}
		suite.T().Run(name, setupTestTeardown)
	}
}

func testMultiSendAndReceive(ctx context.Context, t *testing.T, client *Hub, partitionIDs []string, _ string) {
	numMessages := rand.Intn(100) + 20
	var wg sync.WaitGroup
	wg.Add(numMessages)

	messages := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		messages[i] = test.RandomString("hello", 10)
	}

	for idx, message := range messages {
		if !assert.NoError(t, client.Send(ctx, NewEventFromString(message), SendWithMessageID(fmt.Sprintf("%d", idx)))){
			assert.FailNow(t, "unable to send message")
		}
	}

	for _, partitionID := range partitionIDs {
		_, err := client.Receive(ctx, partitionID, func(ctx context.Context, event *Event) error {
			wg.Done()
			return nil
		}, ReceiveWithPrefetchCount(100))
		if !assert.NoError(t, err) {
			assert.FailNow(t, "unable to setup receiver")
		}
	}
	end, _ := ctx.Deadline()
	waitUntil(t, &wg, time.Until(end))
}

func testSendWithPartitionKeyAndReceive(ctx context.Context, t *testing.T, client *Hub, partitionIDs []string, _ string) {
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
	tests := map[string]func(context.Context, *testing.T, *Hub, []string, string){
		"TestHubRuntimeInformation":          testHubRuntimeInformation,
		"TestHubPartitionRuntimeInformation": testHubPartitionRuntimeInformation,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hub, cleanup := suite.RandomHub()
			defer cleanup()
			client, closer := suite.newClient(t, *hub.Name)
			defer closer()
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()
			testFunc(ctx, t, client, *hub.PartitionIds, *hub.Name)
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func testHubRuntimeInformation(ctx context.Context, t *testing.T, client *Hub, partitionIDs []string, hubName string) {
	info, err := client.GetRuntimeInformation(ctx)
	if assert.NoError(t, err) {
		assert.Equal(t, len(partitionIDs), info.PartitionCount)
		assert.Equal(t, hubName, info.Path)
	}
}

func testHubPartitionRuntimeInformation(ctx context.Context, t *testing.T, client *Hub, partitionIDs []string, hubName string) {
	info, err := client.GetPartitionInformation(ctx, partitionIDs[0])
	if assert.NoError(t, err) {
		assert.Equal(t, hubName, info.HubPath)
		assert.Equal(t, partitionIDs[0], info.PartitionID)
		assert.Equal(t, "-1", info.LastEnqueuedOffset) // brand new, so should be very last
	}
}

func TestEnvironmentalCreation(t *testing.T) {
	os.Setenv("EVENTHUB_NAME", "foo")
	_, err := NewHubFromEnvironment()
	assert.Nil(t, err)
	os.Unsetenv("EVENTHUB_NAME")
}

func (suite *eventHubSuite) newClient(t *testing.T, hubName string, opts ...HubOption) (*Hub, func()) {
	provider, err := aad.NewJWTProvider(aad.JWTProviderWithEnvironmentVars(), aad.JWTProviderWithAzureEnvironment(&suite.Env))
	if !suite.NoError(err) {
		suite.FailNow("unable to make a new JWT provider")
	}
	return suite.newClientWithProvider(t, hubName, provider, opts...)
}

func (suite *eventHubSuite) newClientWithProvider(t *testing.T, hubName string, provider auth.TokenProvider, opts ...HubOption) (*Hub, func()) {
	opts = append(opts, HubWithEnvironment(suite.Env))
	client, err := NewHub(suite.Namespace, hubName, provider, opts...)
	if !suite.NoError(err) {
		suite.FailNow("unable to make a new Hub")
	}
	return client, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Close(ctx)
	}
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
