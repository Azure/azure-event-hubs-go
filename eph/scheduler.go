package eph

import (
	"context"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	timeout = 60 * time.Second
)

type (
	scheduler struct {
		eventHostProcessor *EventProcessorHost
		partitionIDs       []string
		done               func()
	}
)

func newScheduler(ctx context.Context, eventHostProcessor *EventProcessorHost) (*scheduler, error) {
	runtimeInfo, err := eventHostProcessor.client.GetRuntimeInformation(ctx)
	return &scheduler{
		eventHostProcessor: eventHostProcessor,
		partitionIDs:       runtimeInfo.PartitionIDs,
	}, err
}

func (s *scheduler) Run() error {
	ctx, done := context.WithCancel(context.Background())
	s.done = done
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			var ourLeaseCount uint64
			leasesOwnedByOthers := make(map[string]*Lease)
			leaseCtx, cancel := context.WithTimeout(ctx, timeout)
			allLeases, err := s.eventHostProcessor.leaser.GetLeases(leaseCtx)
			cancel()
			if err != nil {
				log.Error(err)
				continue
			}

			for _, lease := range allLeases {
				if lease.IsExpired() {
					leaseCtx, cancel := context.WithTimeout(ctx, timeout)
					lease, err = s.eventHostProcessor.leaser.AcquireLease(leaseCtx, lease)
					cancel()
					if err != nil {
						log.Error(err)
					}

					if lease.OwnedBy(s.eventHostProcessor.name) {
						atomic.AddUint64(&ourLeaseCount, 1)
					} else {
						leasesOwnedByOthers[lease.partitionID] = lease
					}
				}
			}

			for _, _ := range leasesOwnedByOthers {

			}
		}

	}
	return nil
}

func (s *scheduler) Stop() error {
	if s.done != nil {
		s.done()
	}
	return nil
}
