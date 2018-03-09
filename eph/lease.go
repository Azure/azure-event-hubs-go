package eph

import (
	"context"
	"sync/atomic"
	"time"
)

type (
	// Leaser provides the functionality needed to persist and coordinate leases for partitions
	Leaser interface {
		StoreExists(ctx context.Context) (bool, error)
		EnsureStore(ctx context.Context) error
		DeleteStore(ctx context.Context) error
		GetLeases(ctx context.Context) ([]LeaseMarker, error)
		EnsureLease(ctx context.Context, partitionID string) (LeaseMarker, error)
		DeleteLease(ctx context.Context, partitionID string) error
		AcquireLease(ctx context.Context, partitionID string) (LeaseMarker, bool, error)
		RenewLease(ctx context.Context, partitionID string) (LeaseMarker, bool, error)
		ReleaseLease(ctx context.Context, partitionID string) (bool, error)
		UpdateLease(ctx context.Context, partitionID string) (LeaseMarker, bool, error)
	}

	// Lease represents the information needed to coordinate partitions
	Lease struct {
		PartitionID    string
		epoch          int64
		Owner          string
		expirationTime time.Time
	}

	// LeaseMarker provides the functionality expected of a partition lease with an owner
	LeaseMarker interface {
		GetPartitionID() string
		IsExpired() bool
		IsNotOwnedOrExpired() bool
		GetOwner() string
		IncrementEpoch() int64
	}
)

// NewLease constructs a lease given a partitionID
func NewLease(partitionID string) *Lease {
	return &Lease{
		PartitionID: partitionID,
	}
}

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
	return atomic.AddInt64(&l.epoch, 1)
}

// IsNotOwnedOrExpired indicates that the lease has expired and does not owned by a processor
func (l *Lease) IsNotOwnedOrExpired() bool {
	return l.IsExpired() || l.Owner == ""
}

// IsExpired indicates that the lease has expired and is no longer valid
func (l *Lease) IsExpired() bool {
	return time.Now().After(l.expirationTime)
}

func (l *Lease) expireAfter(d time.Duration) {
	l.expirationTime = time.Now().Add(d)
}
