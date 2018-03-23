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
	"math/rand"
	"time"

	"github.com/Azure/azure-event-hubs-go"
	log "github.com/sirupsen/logrus"
)

type (
	leasedReceiver struct {
		handle    *eventhub.ListenerHandle
		processor *EventProcessorHost
		lease     LeaseMarker
		done      func()
	}
)

func newLeasedReceiver(processor *EventProcessorHost, lease LeaseMarker) *leasedReceiver {
	return &leasedReceiver{
		processor: processor,
		lease:     lease,
	}
}

func (lr *leasedReceiver) Run(ctx context.Context) error {
	partitionID := lr.lease.GetPartitionID()
	epoch := lr.lease.GetEpoch()
	lr.dlog("running...")
	ctx, done := context.WithCancel(context.Background())
	lr.done = done
	go lr.periodicallyRenewLease(ctx)
	handle, err := lr.processor.client.Receive(ctx, partitionID, lr.processor.compositeHandlers(), eventhub.ReceiveWithEpoch(epoch))
	if err != nil {
		return err
	}
	lr.handle = handle
	lr.listenForClose()
	return nil
}

func (lr *leasedReceiver) Close() error {
	lr.dlog("closing...")
	if lr.done != nil {
		lr.done()
	}

	if lr.handle != nil {
		return lr.handle.Close()
	}

	return nil
}

func (lr *leasedReceiver) listenForClose() {
	go func() {
		<-lr.handle.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := lr.processor.scheduler.stopReceiver(ctx, lr.lease)
		if err != nil {
			log.Error(err)
		}
	}()
}

func (lr *leasedReceiver) periodicallyRenewLease(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			skew := time.Duration(rand.Intn(1000)-500) * time.Millisecond
			time.Sleep(DefaultLeaseRenewalInterval + skew)
			lease, ok, err := lr.processor.leaser.RenewLease(ctx, lr.lease.GetPartitionID())
			if err != nil {
				lr.processor.scheduler.stopReceiver(ctx, lr.lease)
				log.Error(err)
				continue
			}
			if !ok {
				lr.dlog("can't renew lease...")
				// tell the scheduler we are not able to renew our lease and should stop receiving
				err := lr.processor.scheduler.stopReceiver(ctx, lr.lease)
				if err != nil {
					log.Error(err)
				}
				return
			}
			lr.dlog("lease renewed...")
			lr.lease = lease
		}
	}
}

func (lr *leasedReceiver) dlog(msg string) {
	name := lr.processor.name
	partitionID := lr.lease.GetPartitionID()
	epoch := lr.lease.GetEpoch()
	log.Debugf("eph %q, partition %q, epoch %d: "+msg, name, partitionID, epoch)
}
