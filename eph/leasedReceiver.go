package eph

import (
	"context"
	"time"

	"github.com/Azure/azure-event-hubs-go"
	log "github.com/sirupsen/logrus"
)

const (
	defaultLeaseDuration = 30 * time.Second
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
	log.Debugf("running receiver for partitionID %q", lr.lease.GetPartitionID())
	ctx, done := context.WithCancel(context.Background())
	lr.done = done
	go lr.periodicallyRenewLease(ctx)
	handle, err := lr.processor.client.Receive(ctx, lr.lease.GetPartitionID(), lr.processor.compositeHandlers())
	if err != nil {
		return err
	}
	lr.handle = handle
	return nil
}

func (lr *leasedReceiver) Close() error {
	log.Debugf("closing receiver for partitionID %q", lr.lease.GetPartitionID())
	if lr.done != nil {
		lr.done()
	}

	if lr.handle != nil {
		return lr.handle.Close()
	}

	return nil
}

func (lr *leasedReceiver) periodicallyRenewLease(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:

			lease, ok, err := lr.processor.leaser.RenewLease(ctx, lr.lease.GetPartitionID())
			if err != nil {
				log.Error(err)
				continue
			}
			if !ok {
				log.Debugf("stopping receive for partitionID %q -- can't renew lease", lr.lease.GetPartitionID())
				// tell the scheduler we are not able to renew our lease and should stop receiving
				err := lr.processor.scheduler.stopReceiver(ctx, lr.lease)
				if err != nil {
					log.Error(err)
				}
				return
			}
			log.Debugf("lease renewed for partitionID %q", lr.lease.GetPartitionID())
			// we were able to renew the lease, so save it and continue
			lr.lease = lease
			time.Sleep(defaultLeaseRenewalInterval)
		}
	}
}
