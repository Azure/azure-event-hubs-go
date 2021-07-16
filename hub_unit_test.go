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
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-amqp-common-go/v3"
	"github.com/Azure/azure-amqp-common-go/v3/uuid"
	"github.com/Azure/azure-event-hubs-go/v3/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	// eventHubUtSuite encapsulates a end to end test of Event Hubs with build up and tear down of all EH resources
	eventHubUtSuite struct {
		test.BaseUTSuite
	}
)

const (
	msgCount = 20
)

func TestEHUT(t *testing.T) {
	suite.Run(t, new(eventHubUtSuite))
}

func (suite *eventHubUtSuite) TestWebSocket() {
	tests := map[string]func(context.Context, *testing.T, *Hub, string){
		"UnitTestSendAndReceive": utTestBasicSendAndReceive,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			evtChan := make(map[string]chan *Event, msgCount)
			var intReceiver = func(h *Hub, ctx context.Context, partitionID string, opts ...ReceiveOption) (Receiver, error) {
				r := &mockReceiver{
					hub:           h,
					consumerGroup: "$Default",
					partitionID:   partitionID,
					eventChan:     evtChan,
					cancel:        make(chan int, 1),
				}
				return r, nil
			}
			partitionID := "0"
			sender := &mockSender{
				partitionID: &partitionID,
				Name:        "mockSender",
				eventChan:   evtChan,
				eventCnt:    0,
			}
			client, closer := suite.newTestHub(t, "mockEH", HubWithPartitionedSender(partitionID), HubWithReceiverInit(intReceiver), HubWithSender(sender))
			sender.Hub = client
			defer closer()
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()
			testFunc(ctx, t, client, partitionID)
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func utTestBasicSendAndReceive(ctx context.Context, t *testing.T, client *Hub, partitionID string) {
	numMessages := msgCount
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
		if !assert.NotNil(t, event.SystemProperties) {
			unwindWaitGroup(numMessages-count, &wg)
			return fmt.Errorf("fatal error")
		}
		assert.NotNil(t, event.SystemProperties.EnqueuedTime)
		assert.NotNil(t, event.SystemProperties.Offset)
		assert.NotNil(t, event.SystemProperties.SequenceNumber)
		assert.Equal(t, int64(count), *event.SystemProperties.SequenceNumber)
		count++
		wg.Done()
		return nil
	}, ReceiveWithPrefetchCount(100))
	if assert.NoError(t, err) {
		end, _ := ctx.Deadline()
		waitUntil(t, &wg, time.Until(end))
	}
}

func (suite *eventHubUtSuite) TestMultiPartition() {
	tests := map[string]func(context.Context, *testing.T, *Hub, []string, string){
		"UnitTestMultiSendAndReceive":            utTestMultiSendAndReceive,
		"UnitTestSendWithPartitionKeyAndReceive": utTestSendWithPartitionKeyAndReceive,
	}

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			evtChan := make(map[string]chan *Event, msgCount)
			var intReceiver = func(h *Hub, ctx context.Context, partitionID string, opts ...ReceiveOption) (Receiver, error) {
				r := &mockReceiver{
					hub:           h,
					consumerGroup: "$Default",
					partitionID:   partitionID,
					eventChan:     evtChan,
					cancel:        make(chan int, 1),
				}
				return r, nil
			}
			hubPartitions := []string{"0", "1", "2", "3"}
			sender := &mockSender{
				Name:      "mockSender",
				eventChan: evtChan,
				eventCnt:  0,
			}
			infoManager := &mockHubInformationManager{
				RuntimeInfo: &HubRuntimeInformation{
					PartitionCount: len(hubPartitions),
				},
			}
			client, closer := suite.newTestHub(t, "mockEH", HubWithInformationManager(infoManager), HubWithReceiverInit(intReceiver), HubWithSender(sender))
			sender.Hub = client
			sender.partitionID = client.senderPartitionID
			defer closer()
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()
			testFunc(ctx, t, client, hubPartitions, client.name)
		}
		suite.T().Run(name, setupTestTeardown)
	}
}

func utTestMultiSendAndReceive(ctx context.Context, t *testing.T, client *Hub, partitionIDs []string, _ string) {
	numMessages := msgCount
	var wg sync.WaitGroup
	wg.Add(numMessages)

	messages := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		messages[i] = test.RandomString("hello", 10)
	}

	for idx, message := range messages {
		require.NoError(t, client.Send(ctx, NewEventFromString(message), SendWithMessageID(fmt.Sprintf("%d", idx))))
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

func utTestSendWithPartitionKeyAndReceive(ctx context.Context, t *testing.T, client *Hub, partitionIDs []string, _ string) {
	numMessages := msgCount
	var wg sync.WaitGroup
	wg.Add(numMessages)

	sentEvents := make(map[string]*Event)
	for i := 0; i < numMessages; i++ {
		content := test.RandomNumberString(100)
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

	received := make(map[string]chan *Event)
	for _, pID := range partitionIDs {
		received[pID] = make(chan *Event, numMessages)
	}
	for _, partitionID := range partitionIDs {
		_, err := client.Receive(ctx, partitionID, func(ctx context.Context, event *Event) error {
			defer wg.Done()
			received[partitionID] <- event
			return nil
		}, ReceiveWithPrefetchCount(100))
		if !assert.NoError(t, err) {
			assert.FailNow(t, "failed to receive from partition. "+err.Error())
		}
	}

	// wait for events to arrive
	end, _ := ctx.Deadline()
	if waitUntil(t, &wg, time.Until(end)) {
		// collect all of the partitioned events
		receivedEventsByID := make(map[string]*Event)
		for _, pID := range partitionIDs {
			close(received[pID])
			for event := range received[pID] {
				receivedEventsByID[event.ID] = event
			}
		}

		// verify the sent events have the same partition keys as the received events
		for key, event := range sentEvents {
			assert.Equal(t, event.PartitionKey, receivedEventsByID[key].PartitionKey)
		}
	}
}

func unwindWaitGroup(count int, wg *sync.WaitGroup) {
	for i := 0; i < count; i++ {
		wg.Done()
	}
}

func (suite *eventHubUtSuite) newTestHub(t *testing.T, hubName string, opts ...HubOption) (*Hub, func()) {
	client, err := NewHub(suite.Namespace, hubName, nil, opts...)
	if !suite.NoError(err) {
		suite.FailNow("unable to make a new Hub")
	}
	return client, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Close(ctx)
	}
}

type (
	mockReceiver struct {
		hub           *Hub
		consumerGroup string
		partitionID   string
		eventChan     map[string]chan *Event
		cancel        chan int
		done          func()
		handler       Handler
	}
)

func (r *mockReceiver) Close(ctx context.Context) error {
	r.cancel <- 0
	return nil
}

func (r *mockReceiver) Listen(handler Handler) *ListenerHandle {
	ctx, done := context.WithCancel(context.Background())
	r.done = done
	r.handler = handler
	go r.listenForMessages(ctx)

	return &ListenerHandle{
		r:   r,
		ctx: ctx,
	}
}

func (r *mockReceiver) listenForMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.cancel:
			return
		case msg := <-r.eventChan[r.partitionID]:
			if err := r.handler(ctx, msg); err != nil {
				return
			}
		}
	}
}

func (r *mockReceiver) LastError() error {
	return nil
}

func (r *mockReceiver) GetIdentifier() string {
	return r.partitionID
}

func (r *mockReceiver) Recover(ctx context.Context) error {
	return nil
}

type (
	mockSender struct {
		Hub         *Hub
		partitionID *string
		Name        string
		eventChan   map[string]chan *Event
		eventCnt    int64
	}
)

func (s *mockSender) Recover(ctx context.Context) error {
	return nil
}

func (s *mockSender) Close(ctx context.Context) error {
	return nil
}

func (s *mockSender) Send(ctx context.Context, event *Event, opts ...SendOption) error {

	var partitionID *string = nil

	if nil != event.PartitionKey {
		runtimeInfo, _ := s.Hub.GetRuntimeInformation(ctx)
		hashedPartitionID := test.RandomNumberStringMod(*event.PartitionKey, runtimeInfo.PartitionCount-1)
		partitionID = &hashedPartitionID
	} else if nil != s.partitionID {
		partitionID = s.partitionID
	} else {
		for randPartition := range s.eventChan {
			partitionID = &randPartition
			break
		}
	}
	if nil == partitionID {
		p := "0"
		partitionID = &p
	}
	return s.sendInternal(*partitionID, event)
}

func (s *mockSender) sendInternal(partitionID string, event *Event) error {
	now := time.Now()

	if event.ID == "" {
		id, err := uuid.NewV4()
		if err != nil {
			return err
		}
		event.ID = id.String()
	}

	event.SystemProperties = &SystemProperties{
		EnqueuedTime:   &now,
		SequenceNumber: common.PtrInt64(s.eventCnt),
		Offset:         common.PtrInt64(0),
	}

	if nil == s.eventChan[partitionID] {
		s.eventChan[partitionID] = make(chan *Event, msgCount)
	}
	s.eventChan[partitionID] <- event
	s.eventCnt++
	return nil
}

func (s *mockSender) trySend(ctx context.Context, evt eventer) error {
	return nil
}

func (s *mockSender) String() string {
	return s.Name
}

type (
	mockHubInformationManager struct {
		RuntimeInfo  *HubRuntimeInformation
		ParitionInfo *HubPartitionRuntimeInformation
	}
)

func (m *mockHubInformationManager) GetRuntimeInformation(context.Context) (*HubRuntimeInformation, error) {
	return m.RuntimeInfo, nil
}
func (m *mockHubInformationManager) GetPartitionInformation(context.Context, string) (*HubPartitionRuntimeInformation, error) {
	return m.ParitionInfo, nil
}
