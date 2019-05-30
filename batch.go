package eventhub

import (
	"github.com/Azure/azure-amqp-common-go/uuid"
	"pack.ag/amqp"
)

type (
	// BatchOptions are optional information to add to a batch of messages
	BatchOptions struct {
		MaxSize MaxMessageSizeInBytes
	}

	// BatchIterator offers a simple mechanism for batching a list of events
	BatchIterator interface {
		Done() bool
		Next(messageID string, opts *BatchOptions) (*EventBatch, error)
	}

	// EventBatchIterator provides an easy way to iterate over a slice of events to reliably create batches
	EventBatchIterator struct {
		Events []*Event
		Cursor int
	}

	// EventBatch is a batch of Event Hubs messages to be sent
	EventBatch struct {
		*Event
		marshaledMessages [][]byte
		MaxSize           MaxMessageSizeInBytes
		size              int
	}

	// BatchOption provides a way to configure `BatchOptions`
	BatchOption func(opt *BatchOptions) error

	// MaxMessageSizeInBytes is the max number of bytes allowed by Azure Service Bus
	MaxMessageSizeInBytes uint
)

const (
	// DefaultMaxMessageSizeInBytes is the maximum number of bytes in an event (https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-quotas)
	DefaultMaxMessageSizeInBytes MaxMessageSizeInBytes = 1000000

	batchMessageWrapperSize = 100
)

// BatchWithMaxSizeInBytes configures the EventBatchIterator to fill the batch to the specified max size in bytes
func BatchWithMaxSizeInBytes(sizeInBytes int) BatchOption {
	return func(batchOption *BatchOptions) error {
		batchOption.MaxSize = MaxMessageSizeInBytes(sizeInBytes)
		return nil
	}
}

// NewEventBatchIterator wraps a slice of `Event` pointers to allow it to be made into a `EventBatchIterator`.
func NewEventBatchIterator(events ...*Event) *EventBatchIterator {
	return &EventBatchIterator{
		Events: events,
	}
}

// Done communicates whether there are more messages remaining to be iterated over.
func (ebi *EventBatchIterator) Done() bool {
	return len(ebi.Events) == ebi.Cursor
}

// Next fetches the batch of messages in the message slice at a position one larger than the last one accessed.
func (ebi *EventBatchIterator) Next(eventID string, opts *BatchOptions) (*EventBatch, error) {
	if ebi.Done() {
		return nil, ErrNoMessages{}
	}

	if opts == nil {
		opts = &BatchOptions{
			MaxSize: DefaultMaxMessageSizeInBytes,
		}
	}

	eb := NewEventBatch(eventID, opts)
	for ebi.Cursor < len(ebi.Events) {
		ok, err := eb.Add(ebi.Events[ebi.Cursor])
		if err != nil {
			return nil, err
		}

		if !ok {
			return eb, nil
		}
		ebi.Cursor++
	}
	return eb, nil
}

// NewEventBatch builds a new event batch
func NewEventBatch(eventID string, opts *BatchOptions) *EventBatch {
	if opts == nil {
		opts = &BatchOptions{
			MaxSize: DefaultMaxMessageSizeInBytes,
		}
	}

	mb := &EventBatch{
		MaxSize: opts.MaxSize,
		Event: &Event{
			ID: eventID,
		},
	}

	return mb
}

// Add adds a message to the batch if the message will not exceed the max size of the batch
func (eb *EventBatch) Add(e *Event) (bool, error) {
	e.PartitionKey = eb.PartitionKey

	msg, err := e.toMsg()
	if err != nil {
		return false, err
	}

	if msg.Properties.MessageID == nil || msg.Properties.MessageID == "" {
		uid, err := uuid.NewV4()
		if err != nil {
			return false, err
		}
		msg.Properties.MessageID = uid.String()
	}

	bin, err := msg.MarshalBinary()
	if err != nil {
		return false, err
	}

	if eb.Size()+len(bin) > int(eb.MaxSize) {
		return false, nil
	}

	eb.size += len(bin)
	eb.marshaledMessages = append(eb.marshaledMessages, bin)
	return true, nil
}

// Clear will zero out the batch size and clear the buffered messages
func (eb *EventBatch) Clear() {
	eb.marshaledMessages = [][]byte{}
	eb.size = 0
}

// Size is the number of bytes in the message batch
func (eb *EventBatch) Size() int {
	// calculated data size + batch message wrapper + data wrapper portions of the message
	return eb.size + batchMessageWrapperSize + (len(eb.marshaledMessages) * 5)
}

func (eb *EventBatch) toMsg() (*amqp.Message, error) {
	batchMessage := eb.amqpBatchMessage()

	batchMessage.Data = make([][]byte, len(eb.marshaledMessages))
	for idx, bytes := range eb.marshaledMessages {
		batchMessage.Data[idx] = bytes
	}
	return batchMessage, nil
}

func (eb *EventBatch) amqpBatchMessage() *amqp.Message {
	return &amqp.Message{
		Data:   make([][]byte, len(eb.marshaledMessages)),
		Format: batchMessageFormat,
		Properties: &amqp.MessageProperties{
			MessageID: eb.ID,
		},
	}
}
