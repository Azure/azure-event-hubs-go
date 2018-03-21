package eph

import (
	"context"
	"io"

	"github.com/Azure/azure-amqp-common-go/persist"
)

type (
	// Checkpointer interface provides the ability to persist durable checkpoints for event processors
	Checkpointer interface {
		io.Closer
		StoreProvisioner
		EventProcessHostSetter
		GetCheckpoint(ctx context.Context, partitionID string) (persist.Checkpoint, bool)
		EnsureCheckpoint(ctx context.Context, partitionID string) (persist.Checkpoint, error)
		UpdateCheckpoint(ctx context.Context, partitionID string, checkpoint persist.Checkpoint) error
		DeleteCheckpoint(ctx context.Context, partitionID string) error
	}
)
