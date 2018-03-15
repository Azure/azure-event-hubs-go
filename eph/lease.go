package eph

import (
	"context"
	"sync/atomic"
)

type (
	// StoreProvisioner provides CRUD functionality for Lease and Checkpoint storage
	StoreProvisioner interface {
		StoreExists(ctx context.Context) (bool, error)
		EnsureStore(ctx context.Context) error
		DeleteStore(ctx context.Context) error
	}

	// Leaser provides the functionality needed to persist and coordinate leases for partitions
	Leaser interface {
		StoreProvisioner
		GetLeases(ctx context.Context) ([]LeaseMarker, error)
		EnsureLease(ctx context.Context, partitionID string) (LeaseMarker, error)
		DeleteLease(ctx context.Context, partitionID string) error
		AcquireLease(ctx context.Context, partitionID string) (LeaseMarker, bool, error)
		RenewLease(ctx context.Context, partitionID string) (LeaseMarker, bool, error)
		ReleaseLease(ctx context.Context, partitionID string) (bool, error)
		UpdateLease(ctx context.Context, partitionID string) (LeaseMarker, bool, error)
		SetEventHostProcessor(eph *EventProcessorHost)
	}

	// Lease represents the information needed to coordinate partitions
	Lease struct {
		PartitionID string `json:"partitionID"`
		Epoch       int64  `json:"epoch"`
		Owner       string `json:"owner"`
	}

	// LeaseMarker provides the functionality expected of a partition lease with an owner
	LeaseMarker interface {
		GetPartitionID() string
		IsExpired(context.Context) bool
		GetOwner() string
		IncrementEpoch() int64
		GetEpoch() int64
	}
)

// GetPartitionID returns the partition which belongs to this lease
func (l *Lease) GetPartitionID() string {
	return l.PartitionID
}

// GetOwner returns the owner of the lease
func (l *Lease) GetOwner() string {
	return l.Owner
}

// IncrementEpoch increase the time on the lease by one
func (l *Lease) IncrementEpoch() int64 {
	return atomic.AddInt64(&l.Epoch, 1)
}

// GetEpoch returns the value of the epoch
func (l *Lease) GetEpoch() int64 {
	return l.Epoch
}
