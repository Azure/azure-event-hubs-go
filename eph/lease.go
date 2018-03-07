package eph

import (
	"context"
	"sync/atomic"
)

type (
	// Leaser provides the functionality needed to persist and coordinate leases for partitions
	Leaser interface {
		StoreExists(ctx context.Context) (bool, error)
		EnsureStore(ctx context.Context) error
		DeleteStore(ctx context.Context) error
		GetLeases(ctx context.Context) ([]Lease, error)
		EnsureLease(ctx context.Context, lease Lease) (Lease, error)
		DeleteLease(ctx context.Context, lease Lease) error
		AcquireLease(ctx context.Context, lease Lease) (Lease, bool, error)
		RenewLease(ctx context.Context, lease Lease) (Lease, bool, error)
		ReleaseLease(ctx context.Context, lease Lease) (bool, error)
		UpdateLease(ctx context.Context, lease Lease) (Lease, bool, error)
	}

	// Lease represents the information needed to coordinate partitions
	Lease struct {
		PartitionID string
		epoch       int64
		Owner       string
	}
)

// NewLease constructs a lease given a partitionID
func NewLease(partitionID string) *Lease {
	return &Lease{
		PartitionID: partitionID,
	}
}

// IncrementEpoch increase the time on the lease by one
func (l *Lease) IncrementEpoch() int64 {
	return atomic.AddInt64(&l.epoch, 1)
}

// IsExpired indicates that the lease has expired and is no longer valid
func (l *Lease) IsExpired() bool {
	return false
}

// IsNotOwnedOrExpired indicates that the lease has expired and does not owned by a processor
func (l *Lease) IsNotOwnedOrExpired() bool {
	return l.IsExpired() || l.Owner == ""
}
