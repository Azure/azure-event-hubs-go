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
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/devigned/tab"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
)

type (
	leasedReceiver struct {
		handle    *eventhub.ListenerHandle
		processor *EventProcessorHost
		lease     *atomic.Value // LeaseMarker
		done      func()
	}
)

func newLeasedReceiver(processor *EventProcessorHost, lease LeaseMarker) *leasedReceiver {
	leaseValue := atomic.Value{}
	leaseValue.Store(lease)

	return &leasedReceiver{
		processor: processor,
		lease:     &leaseValue,
	}
}

func (lr *leasedReceiver) Run(ctx context.Context) error {
	span, ctx := lr.startConsumerSpanFromContext(ctx, "eph.leasedReceiver.Run")
	defer span.End()

	lease := lr.getLease()

	partitionID := lease.GetPartitionID()
	epoch := lease.GetEpoch()
	lr.dlog(ctx, "running...")

	renewLeaseCtx, cancelRenewLease := context.WithCancel(context.Background())
	lr.done = cancelRenewLease

	go lr.periodicallyRenewLease(renewLeaseCtx)

	opts := []eventhub.ReceiveOption{eventhub.ReceiveWithEpoch(epoch)}
	if lr.processor.consumerGroup != "" {
		opts = append(opts, eventhub.ReceiveWithConsumerGroup(lr.processor.consumerGroup))
	}

	handle, err := lr.processor.client.Receive(ctx, partitionID, lr.processor.compositeHandlers(), opts...)
	if err != nil {
		return err
	}
	lr.handle = handle
	lr.listenForClose()
	return nil
}

func (lr *leasedReceiver) Close(ctx context.Context) error {
	span, ctx := lr.startConsumerSpanFromContext(ctx, "eph.leasedReceiver.Close")
	defer span.End()

	if lr.done != nil {
		lr.done()
	}

	if lr.handle != nil {
		return lr.handle.Close(ctx)
	}

	return nil
}

func (lr *leasedReceiver) listenForClose() {
	go func() {
		<-lr.handle.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		span, ctx := lr.startConsumerSpanFromContext(ctx, "eph.leasedReceiver.listenForClose")
		defer span.End()

		lease := lr.getLease()
		err := lr.processor.scheduler.stopReceiver(ctx, lease)
		if err != nil {
			tab.For(ctx).Error(err)
		}
	}()
}

func (lr *leasedReceiver) periodicallyRenewLease(ctx context.Context) {
	span, ctx := lr.startConsumerSpanFromContext(ctx, "eph.leasedReceiver.periodicallyRenewLease")
	defer span.End()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(DefaultLeaseRenewalInterval + (time.Duration(rand.Intn(1000)-500) * time.Millisecond)):
			err := lr.tryRenew(ctx)
			if err != nil {
				tab.For(ctx).Error(err)
				lease := lr.getLease()
				// the passed in context gets cancelled when we want the periodic lease renewal to stop.
				// we can't pass it to stopReceiver() as that's guaranteed to not work.
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				_ = lr.processor.scheduler.stopReceiver(ctx, lease)
				cancel()
			}
		}
	}
}

func (lr *leasedReceiver) tryRenew(ctx context.Context) error {
	span, ctx := lr.startConsumerSpanFromContext(ctx, "eph.leasedReceiver.tryRenew")
	defer span.End()

	oldLease := lr.getLease()
	lease, ok, err := lr.processor.leaser.RenewLease(ctx, oldLease.GetPartitionID())
	if err != nil {
		tab.For(ctx).Error(err)
		return err
	}
	if !ok {
		err = errors.New("can't renew lease")
		tab.For(ctx).Error(err)
		return err
	}
	lr.dlog(ctx, "lease renewed")

	lr.lease.Store(lease)
	return nil
}

func (lr *leasedReceiver) dlog(ctx context.Context, msg string) {
	name := lr.processor.name
	lease := lr.getLease()

	partitionID := lease.GetPartitionID()
	epoch := lease.GetEpoch()
	tab.For(ctx).Debug(fmt.Sprintf("eph %q, partition %q, epoch %d: "+msg, name, partitionID, epoch))
}

func (lr *leasedReceiver) startConsumerSpanFromContext(ctx context.Context, operationName string) (tab.Spanner, context.Context) {
	span, ctx := startConsumerSpanFromContext(ctx, operationName)

	lease := lr.getLease()

	span.AddAttributes(
		tab.StringAttribute("eph.id", lr.processor.name),
		tab.StringAttribute(partitionIDTag, lease.GetPartitionID()),
		tab.Int64Attribute(epochTag, lease.GetEpoch()),
	)
	return span, ctx
}

func (lr *leasedReceiver) getLease() LeaseMarker {
	return lr.lease.Load().(LeaseMarker)
}
