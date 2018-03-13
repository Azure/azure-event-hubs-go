package eph

import (
	"context"
)

type (
	// Checkpointer interface provides the ability to persist durable checkpoints for event processors
	Checkpointer interface {
		StoreProvisioner
		GetCheckpoint(ctx context.Context, partitionID string) (Checkpoint, bool)
		EnsureCheckpoint(ctx context.Context, partitionID string) (Checkpoint, error)
		UpdateCheckpoint(ctx context.Context, checkpoint Checkpoint) error
		DeleteCheckpoint(ctx context.Context, partitionID string) error
		SetEventHostProcessor(eph *EventProcessorHost)
	}

	// Checkpoint is the information needed to determine the last message processed
	Checkpoint struct {
		PartitionID    string `json:"partitionID"`
		Offset         string `json:"offset"`
		SequenceNumber int64  `json:"sequenceNumber"`
	}
)

// NewCheckpoint constructs a checkpoint given a partitionID
func NewCheckpoint(partitionID string) *Checkpoint {
	return &Checkpoint{
		PartitionID: partitionID,
	}
}
