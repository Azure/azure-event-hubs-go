package eph

import (
	"context"
)

type (
	Checkpointer interface {
		StoreExists(ctx context.Context) (bool, error)
		EnsureStore(ctx context.Context) error
		DeleteStore(ctx context.Context) error
		GetCheckpoint(ctx context.Context, partitionID string) (*Checkpoint, error)
		EnsureCheckpoint(ctx context.Context, checkpoint *Checkpoint) (*Checkpoint, error)
		UpdateCheckpoint(ctx context.Context, checkpoint *Checkpoint) error
		DeleteCheckpoint(ctx context.Context, partitionID string) error
	}

	Checkpoint struct {
		partitionID    string
		offset         string
		sequenceNumber int64
	}
)

func NewCheckpoint(partitionID string) *Checkpoint {
	return &Checkpoint{
		partitionID: partitionID,
	}
}
