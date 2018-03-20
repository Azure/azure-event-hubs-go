package eph

import (
	"context"
	"math/rand"
	"time"

	"github.com/Azure/azure-event-hubs-go"
	log "github.com/sirupsen/logrus"
)

type (
	leasedReceiver struct {
		handle    eventhub.ListenerHandle
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
