package eventhub

import (
	"pack.ag/amqp"
)

const (
	batchMessageFormat uint32 = 0x80013700
)

type (
	// Event is an Event Hubs message to be sent or received
	Event struct {
		Data    []byte
		message *amqp.Message
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

func newEvent(data []byte, msg *amqp.Message) *Event {
	return &Event{
		Data:    data,
		message: msg,
	}
}

func (e *Event) toMsg() *amqp.Message {
	return amqp.NewMessage(e.Data)
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
