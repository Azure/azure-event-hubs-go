package eph

import (
	"context"
	"sync/atomic"
)

type (
	Leaser interface {
		StoreExists(ctx context.Context) (bool, error)
		EnsureStore(ctx context.Context) error
		DeleteStore(ctx context.Context) error
		GetLeases(ctx context.Context) ([]*Lease, error)
		EnsureLease(ctx context.Context, lease *Lease) (*Lease, error)
		DeleteLease(ctx context.Context, lease *Lease) error
		AcquireLease(ctx context.Context, lease *Lease) (*Lease, error)
		RenewLease(ctx context.Context, lease *Lease) (*Lease, error)
		ReleaseLease(ctx context.Context, lease *Lease) error
		UpdateLease(ctx context.Context, lease *Lease) (*Lease, error)
	}

	Lease struct {
		partitionID string
		epoch       int64
		owner       string
		token       string
	}
)

func NewLease(partitionID string) *Lease {
	return &Lease{
		partitionID: partitionID,
	}
}

func (l *Lease) IncrementEpoch() int64 {
	return atomic.AddInt64(&l.epoch, 1)
}

func (l *Lease) IsExpired() bool {
	return false
}

func (l *Lease) OwnedBy(name string) bool {
	return l.owner == name
}
