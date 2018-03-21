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
	"time"

	"github.com/Azure/azure-amqp-common-go/persist"
	"pack.ag/amqp"
)

const (
	batchMessageFormat         uint32 = 0x80013700
	partitionKeyAnnotationName string = "x-opt-partition-key"
	sequenceNumberName         string = "x-opt-sequence-number"
	enqueueTimeName            string = "x-opt-enqueued-time"
)

type (
	// Event is an Event Hubs message to be sent or received
	Event struct {
		Data         []byte
		PartitionKey *string
		message      *amqp.Message
	}

	// EventBatch is a batch of Event Hubs messages to be sent
	EventBatch struct {
		Events []*Event
	}
)

// NewEventFromString builds an Event from a string message
func NewEventFromString(message string) *Event {
	return &Event{
		Data: []byte(message),
	}
}

// NewEvent builds an Event from a slice of data
func NewEvent(data []byte) *Event {
	return &Event{
		Data: data,
	}
}

// NewEventBatch builds an EventBatch from an array of Events
func NewEventBatch(events []*Event) *EventBatch {
	return &EventBatch{
		Events: events,
	}
}

func newEvent(data []byte, msg *amqp.Message) *Event {
	return &Event{
		Data:    data,
		message: msg,
	}
}

// GetCheckpoint returns the checkpoint information on the Event
func (e *Event) GetCheckpoint() persist.Checkpoint {
	var offset string
	var enqueueTime time.Time
	var sequenceNumber int64
	if val, ok := e.message.Annotations[offsetAnnotationName]; ok {
		offset = val.(string)
	}

	if val, ok := e.message.Annotations[enqueueTimeName]; ok {
		enqueueTime = val.(time.Time)
	}

	if val, ok := e.message.Annotations[sequenceNumberName]; ok {
		sequenceNumber = val.(int64)
	}

	return persist.NewCheckpoint(offset, sequenceNumber, enqueueTime)
}

func (e *Event) toMsg() *amqp.Message {
	msg := amqp.NewMessage(e.Data)
	if e.PartitionKey != nil {
		msg.Annotations[partitionKeyAnnotationName] = e.PartitionKey
	}
	return msg
}

func (b *EventBatch) toMsg() (*amqp.Message, error) {
	msg := new(amqp.Message)
	data := make([][]byte, len(b.Events))
	for idx, event := range b.Events {
		innerMsg := amqp.NewMessage(event.Data)
		bin, err := innerMsg.MarshalBinary()
		if err != nil {
			return nil, err
		}
		data[idx] = bin
	}
	msg.Data = data
	msg.Format = batchMessageFormat
	return msg, nil
}

func eventFromMsg(msg *amqp.Message) *Event {
	return newEvent(msg.Data[0], msg)
}
