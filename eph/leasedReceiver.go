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
		closeReceiver eventhub.ListenerHandle
		processor     *EventProcessorHost
		lease         *Lease
		done          func()
	}
)

func newLeasedReceiver(processor *EventProcessorHost, lease *Lease) *leasedReceiver {
	return &leasedReceiver{
		processor: processor,
		lease:     lease,
	}
}

func (lr *leasedReceiver) Run(ctx context.Context) error {
	ctx, done := context.WithCancel(context.Background())
	lr.done = done
	go lr.periodicallyRenewLease(ctx)
	closer, err := lr.processor.client.Receive(ctx, lr.lease.PartitionID, lr.processor.compositeHandlers())
	if err != nil {
		return err
	}
	lr.closeReceiver = closer
	return nil
}

func (lr *leasedReceiver) Close() error {
	if lr.done != nil {
		lr.done()
	}

	if lr.closeReceiver != nil {
		return lr.closeReceiver.Close()
	}

	return nil
}

func (lr *leasedReceiver) periodicallyRenewLease(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			lease, ok, err := lr.processor.leaser.RenewLease(ctx, *lr.lease)
			if err != nil {
				log.Error(err)
				continue
			}
			if !ok {
				// tell the scheduler we are not able to renew our lease and should stop receiving
				err := lr.processor.scheduler.stopReceiver(ctx, lr.lease)
				if err != nil {
					log.Error(err)
				}
				return
			}
			// we were able to renew the lease, so save it and continue
			lr.lease = &lease
			time.Sleep(defaultLeaseRenewalInterval)
		}
	}
}
