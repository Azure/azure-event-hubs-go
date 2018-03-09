package eph

import (
	"context"
)

type (
	// Checkpointer interface provides the ability to persist durable checkpoints for event processors
	Checkpointer interface {
		StoreExists(ctx context.Context) (bool, error)
		EnsureStore(ctx context.Context) error
		DeleteStore(ctx context.Context) error
		GetCheckpoint(ctx context.Context, partitionID string) (Checkpoint, bool)
		EnsureCheckpoint(ctx context.Context, partitionID string) (Checkpoint, error)
		UpdateCheckpoint(ctx context.Context, checkpoint Checkpoint) error
		DeleteCheckpoint(ctx context.Context, partitionID string) error
	}

	// Checkpoint is the information needed to determine the last message processed
	Checkpoint struct {
		partitionID    string
		offset         string
		sequenceNumber int64
	}
)

// NewCheckpoint constructs a checkpoint given a partitionID
func NewCheckpoint(partitionID string) *Checkpoint {
	return &Checkpoint{
		partitionID: partitionID,
	}
}
