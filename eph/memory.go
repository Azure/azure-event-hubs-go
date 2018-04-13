package eph

//	MIT License
//
//	Copyright (c) Microsoft Corporation. All rights reserved.
//
//	Permission is hereby granted, free of charge, to any person obtaining a copy
//	of this software and associated documentation files (the "Software"), to deal
//	in the Software without restriction, including without limitation the rights
//	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//	copies of the Software, and to permit persons to whom the Software is
//	furnished to do so, subject to the following conditions:
//
//	The above copyright notice and this permission notice shall be included in all
//	copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//	SOFTWARE

import (
	"context"
	"sync"
	"time"

	"github.com/Azure/azure-amqp-common-go/persist"
	"github.com/pkg/errors"
)

type (
	memoryLeaser struct {
		leases        map[string]*memoryLease
		ownerName     string
		leaseDuration time.Duration
		memMu         sync.Mutex
	}

	memoryCheckpointer struct {
		checkpoints map[string]*persist.Checkpoint
		processor   *EventProcessorHost
		memMu       sync.Mutex
	}

	memoryLease struct {
		Lease
		expirationTime time.Time
	}
)

func newMemoryLease(partitionID string) *memoryLease {
	lease := new(memoryLease)
	lease.PartitionID = partitionID
	return lease
}

// IsNotOwnedOrExpired indicates that the lease has expired and does not owned by a processor
func (l *memoryLease) isNotOwnedOrExpired(ctx context.Context) bool {
	return l.IsExpired(ctx) || l.Owner == ""
}

// IsExpired indicates that the lease has expired and is no longer valid
func (l *memoryLease) IsExpired(_ context.Context) bool {
	return time.Now().After(l.expirationTime)
}

func (l *memoryLease) expireAfter(d time.Duration) {
	l.expirationTime = time.Now().Add(d)
}

func newMemoryLeaser(leaseDuration time.Duration) Leaser {
	return &memoryLeaser{
		leaseDuration: leaseDuration,
	}
}

func (ml *memoryLeaser) SetEventHostProcessor(eph *EventProcessorHost) {
	ml.ownerName = eph.name
}

func (ml *memoryLeaser) StoreExists(ctx context.Context) (bool, error) {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryLeaser.StoreExists")
	defer span.Finish()

	return ml.leases != nil, nil
}

func (ml *memoryLeaser) EnsureStore(ctx context.Context) error {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryLeaser.EnsureStore")
	defer span.Finish()

	if ml.leases == nil {
		ml.leases = make(map[string]*memoryLease)
	}
	return nil
}

func (ml *memoryLeaser) DeleteStore(ctx context.Context) error {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryLeaser.DeleteStore")
	defer span.Finish()

	return ml.EnsureStore(ctx)
}

func (ml *memoryLeaser) GetLeases(ctx context.Context) ([]LeaseMarker, error) {
	ml.memMu.Lock()
	defer ml.memMu.Unlock()

	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryLeaser.GetLeases")
	defer span.Finish()

	leases := make([]LeaseMarker, len(ml.leases))
	count := 0
	for _, lease := range ml.leases {
		leases[count] = lease
		count++
	}
	return leases, nil
}

func (ml *memoryLeaser) EnsureLease(ctx context.Context, partitionID string) (LeaseMarker, error) {
	ml.memMu.Lock()
	defer ml.memMu.Unlock()

	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryLeaser.EnsureLease")
	defer span.Finish()

	l, ok := ml.leases[partitionID]
	if !ok {
		l = newMemoryLease(partitionID)
		ml.leases[l.PartitionID] = l
	}
	return l, nil
}

func (ml *memoryLeaser) DeleteLease(ctx context.Context, partitionID string) error {
	ml.memMu.Lock()
	defer ml.memMu.Unlock()

	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryLeaser.DeleteLease")
	defer span.Finish()

	delete(ml.leases, partitionID)
	return nil
}

func (ml *memoryLeaser) AcquireLease(ctx context.Context, partitionID string) (LeaseMarker, bool, error) {
	ml.memMu.Lock()
	defer ml.memMu.Unlock()

	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryLeaser.AcquireLease")
	defer span.Finish()

	l, ok := ml.leases[partitionID]
	if !ok {
		// lease is not in store
		return nil, false, errors.New("lease is not in the store")
	}

	if l.isNotOwnedOrExpired(ctx) || l.Owner != ml.ownerName {
		// no one owns it or owned by someone else
		l.Owner = ml.ownerName
	}
	l.expireAfter(ml.leaseDuration)
	l.IncrementEpoch()
	return l, true, nil
}

func (ml *memoryLeaser) RenewLease(ctx context.Context, partitionID string) (LeaseMarker, bool, error) {
	ml.memMu.Lock()
	defer ml.memMu.Unlock()

	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryLeaser.RenewLease")
	defer span.Finish()

	l, ok := ml.leases[partitionID]
	if !ok {
		// lease is not in store
		return nil, false, errors.New("lease is not in the store")
	}

	if l.Owner != ml.ownerName {
		return nil, false, nil
	}

	l.expireAfter(ml.leaseDuration)
	return l, true, nil
}

func (ml *memoryLeaser) ReleaseLease(ctx context.Context, partitionID string) (bool, error) {
	ml.memMu.Lock()
	defer ml.memMu.Unlock()

	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryLeaser.ReleaseLease")
	defer span.Finish()

	l, ok := ml.leases[partitionID]
	if !ok {
		// lease is not in store
		return false, errors.New("lease is not in the store")
	}

	if l.IsExpired(ctx) || l.Owner != ml.ownerName {
		return false, nil
	}

	l.Owner = ""
	l.expirationTime = time.Now().Add(-1 * time.Second)

	return false, nil
}

func (ml *memoryLeaser) UpdateLease(ctx context.Context, partitionID string) (LeaseMarker, bool, error) {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryLeaser.UpdateLease")
	defer span.Finish()

	l, ok, err := ml.RenewLease(ctx, partitionID)

	ml.memMu.Lock()
	defer ml.memMu.Unlock()

	if err != nil || !ok {
		return nil, ok, err
	}
	l.IncrementEpoch()
	return l, true, nil
}

func (mc *memoryCheckpointer) SetEventHostProcessor(eph *EventProcessorHost) {
	// no op
}

func (mc *memoryCheckpointer) StoreExists(ctx context.Context) (bool, error) {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryCheckpointer.StoreExists")
	defer span.Finish()

	return mc.checkpoints == nil, nil
}

func (mc *memoryCheckpointer) EnsureStore(ctx context.Context) error {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryCheckpointer.EnsureStore")
	defer span.Finish()

	if mc.checkpoints == nil {
		mc.checkpoints = make(map[string]*persist.Checkpoint)
	}
	return nil
}

func (mc *memoryCheckpointer) DeleteStore(ctx context.Context) error {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryCheckpointer.DeleteStore")
	defer span.Finish()

	mc.checkpoints = nil
	return nil
}

func (mc *memoryCheckpointer) GetCheckpoint(ctx context.Context, partitionID string) (persist.Checkpoint, bool) {
	mc.memMu.Lock()
	defer mc.memMu.Unlock()

	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryCheckpointer.GetCheckpoint")
	defer span.Finish()

	checkpoint, ok := mc.checkpoints[partitionID]
	if !ok {
		return *new(persist.Checkpoint), ok
	}

	return *checkpoint, true
}

func (mc *memoryCheckpointer) EnsureCheckpoint(ctx context.Context, partitionID string) (persist.Checkpoint, error) {
	mc.memMu.Lock()
	defer mc.memMu.Unlock()

	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryCheckpointer.EnsureCheckpoint")
	defer span.Finish()

	checkpoint, ok := mc.checkpoints[partitionID]
	if !ok {
		chkpnt := persist.NewCheckpointFromStartOfStream()
		checkpoint = &chkpnt
		mc.checkpoints[partitionID] = checkpoint
	}
	return *checkpoint, nil
}

func (mc *memoryCheckpointer) UpdateCheckpoint(ctx context.Context, partitionID string, checkpoint persist.Checkpoint) error {
	mc.memMu.Lock()
	defer mc.memMu.Unlock()

	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryCheckpointer.UpdateCheckpoint")
	defer span.Finish()

	if cp, ok := mc.checkpoints[partitionID]; ok {
		checkpoint.SequenceNumber = cp.SequenceNumber
		checkpoint.Offset = cp.Offset
	}
	return nil
}

func (mc *memoryCheckpointer) DeleteCheckpoint(ctx context.Context, partitionID string) error {
	mc.memMu.Lock()
	defer mc.memMu.Unlock()

	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.eph.memoryCheckpointer.DeleteCheckpoint")
	defer span.Finish()

	delete(mc.checkpoints, partitionID)
	return nil
}

func (ml *memoryLeaser) Close() error {
	return nil
}

func (mc *memoryCheckpointer) Close() error {
	return nil
}
