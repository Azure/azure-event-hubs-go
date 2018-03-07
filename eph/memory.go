package eph

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

type (
	memoryLease struct {
		*Lease
		expirationTime time.Time
	}

	memoryLeaser struct {
		leases        map[string]*memoryLease
		processor     *EventProcessorHost
		leaseDuration time.Duration
	}

	memoryCheckpointer struct {
		checkpoints map[string]*Checkpoint
		processor   *EventProcessorHost
	}
)

func newMemoryLease(lease *Lease) *memoryLease {
	return &memoryLease{
		Lease: lease,
	}
}

func (l *memoryLease) ExpireAfter(d time.Duration) {
	l.expirationTime = time.Now().Add(d)
}

func (l *memoryLease) IsExpired() bool {
	return time.Now().After(l.expirationTime)
}

func newMemoryLeaser(processor *EventProcessorHost, leaseDuration time.Duration) *memoryLeaser {
	return &memoryLeaser{
		processor:     processor,
		leaseDuration: leaseDuration,
	}
}

func (ml *memoryLeaser) StoreExists(ctx context.Context) (bool, error) {
	return ml.leases != nil, nil
}

func (ml *memoryLeaser) EnsureStore(ctx context.Context) error {
	if ml.leases == nil {
		ml.leases = make(map[string]*memoryLease)
	}
	return nil
}

func (ml *memoryLeaser) DeleteStore(ctx context.Context) error {
	return ml.EnsureStore(ctx)
}

func (ml *memoryLeaser) GetLeases(ctx context.Context) ([]Lease, error) {
	leases := make([]Lease, len(ml.leases))
	count := 0
	for _, lease := range ml.leases {
		leases[count] = *lease.Lease
		count++
	}
	return leases, nil
}

func (ml *memoryLeaser) EnsureLease(ctx context.Context, partitionID string) (Lease, error) {
	l, ok := ml.leases[partitionID]
	if !ok {
		l = newMemoryLease(NewLease(partitionID))
		ml.leases[l.PartitionID] = l
	}
	return *l.Lease, nil
}

func (ml *memoryLeaser) DeleteLease(ctx context.Context, lease Lease) error {
	delete(ml.leases, lease.PartitionID)
	return nil
}

func (ml *memoryLeaser) AcquireLease(ctx context.Context, partitionID string) (Lease, bool, error) {
	l, ok := ml.leases[partitionID]
	if !ok {
		// lease is not in store
		return Lease{}, false, errors.New("lease is not in the store")
	}

	if l.IsNotOwnedOrExpired() || l.Owner != ml.processor.name {
		// no one owns it or owned by someone else
		l.Owner = ml.processor.name
	}
	l.ExpireAfter(ml.leaseDuration)
	return *l.Lease, true, nil
}

func (ml *memoryLeaser) RenewLease(ctx context.Context, partitionID string) (Lease, bool, error) {
	l, ok := ml.leases[partitionID]
	if !ok {
		// lease is not in store
		return Lease{}, false, errors.New("lease is not in the store")
	}

	if l.Owner != ml.processor.name {
		return Lease{}, false, nil
	}

	l.ExpireAfter(ml.leaseDuration)
	return *l.Lease, true, nil
}

func (ml *memoryLeaser) ReleaseLease(ctx context.Context, partitionID string) (bool, error) {
	l, ok := ml.leases[partitionID]
	if !ok {
		// lease is not in store
		return false, errors.New("lease is not in the store")
	}

	if l.IsExpired() || l.Owner != ml.processor.name {
		return false, nil
	}

	l.Owner = ""
	l.expirationTime = time.Now().Add(-1 * time.Second)

	return false, nil
}

func (ml *memoryLeaser) UpdateLease(ctx context.Context, partitionID string) (Lease, bool, error) {
	l, ok, err := ml.RenewLease(ctx, partitionID)
	if err != nil || !ok {
		return Lease{}, ok, err
	}
	l.IncrementEpoch()
	return l, true, nil
}

func newMemoryCheckpointer(processor *EventProcessorHost) *memoryCheckpointer {
	return &memoryCheckpointer{
		processor: processor,
	}
}

func (mc *memoryCheckpointer) StoreExists(ctx context.Context) (bool, error) {
	return mc.checkpoints == nil, nil
}

func (mc *memoryCheckpointer) EnsureStore(ctx context.Context) error {
	if mc.checkpoints == nil {
		mc.checkpoints = make(map[string]*Checkpoint)
	}
	return nil
}

func (mc *memoryCheckpointer) DeleteStore(ctx context.Context) error {
	mc.checkpoints = nil
	return nil
}

func (mc *memoryCheckpointer) GetCheckpoint(ctx context.Context, partitionID string) (Checkpoint, bool) {
	checkpoint, ok := mc.checkpoints[partitionID]
	if !ok {
		return *new(Checkpoint), ok
	}

	return *checkpoint, true
}

func (mc *memoryCheckpointer) EnsureCheckpoint(ctx context.Context, partitionID string) (Checkpoint, error) {
	checkpoint, ok := mc.checkpoints[partitionID]
	if !ok {
		checkpoint = NewCheckpoint(partitionID)
		mc.checkpoints[partitionID] = checkpoint
	}
	return *checkpoint, nil
}

func (mc *memoryCheckpointer) UpdateCheckpoint(ctx context.Context, checkpoint Checkpoint) error {
	if cp, ok := mc.checkpoints[checkpoint.partitionID]; ok {
		checkpoint.sequenceNumber = cp.sequenceNumber
		checkpoint.offset = cp.offset
	}
	return nil
}

func (mc *memoryCheckpointer) DeleteCheckpoint(ctx context.Context, partitionID string) error {
	delete(mc.checkpoints, partitionID)
	return nil
}
