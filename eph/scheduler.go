package eph

import (
	"context"
	"time"

	"github.com/Azure/azure-event-hubs-go"
	log "github.com/sirupsen/logrus"
)

var (
	timeout = 60 * time.Second
)

const (
	defaultLeaseRenewalInterval = 10 * time.Second
)

type (
	scheduler struct {
		processor    *EventProcessorHost
		partitionIDs []string
		receivers    map[string]*leasedReceiver
		done         func()
	}

	ownerCount struct {
		Owner  string
		Leases []*Lease
	}
)

func newScheduler(ctx context.Context, eventHostProcessor *EventProcessorHost) (*scheduler, error) {
	runtimeInfo, err := eventHostProcessor.client.GetRuntimeInformation(ctx)
	return &scheduler{
		processor:    eventHostProcessor,
		partitionIDs: runtimeInfo.PartitionIDs,
	}, err
}

func (s *scheduler) Run() {
	ctx, done := context.WithCancel(context.Background())
	s.done = done
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// fetch updated view of all leases
			leaseCtx, cancel := context.WithTimeout(ctx, timeout)
			allLeases, err := s.processor.leaser.GetLeases(leaseCtx)
			cancel()
			if err != nil {
				log.Error(err)
				continue
			}

			// try to acquire any leases that have expired
			acquired, notAcquired, err := s.acquireExpiredLeases(ctx, allLeases)
			if err != nil {
				log.Error(err)
				continue
			}

			// start receiving message from newly acquired partitions
			for _, lease := range acquired {
				s.startReceiver(ctx, lease)
			}

			// calculate the number of leases we own including the newly acquired partitions
			byOwner := leasesByOwner(notAcquired)
			var countOwnedByMe int
			if val, ok := byOwner[s.processor.name]; ok {
				countOwnedByMe = len(val)
			}
			countOwnedByMe += len(acquired)

			// gather all of the leases owned by others
			var leasesOwnedByOthers []*Lease
			for key, value := range byOwner {
				if key != s.processor.name {
					leasesOwnedByOthers = append(leasesOwnedByOthers, value...)
				}
			}

			// try to steal work away from others if work has become imbalanced
			if candidate, ok := leaseToSteal(leasesOwnedByOthers, countOwnedByMe); ok {
				acquireCtx, cancel := context.WithTimeout(ctx, timeout)
				stolen, ok, err := s.processor.leaser.AcquireLease(acquireCtx, candidate)
				cancel()
				if err != nil {
					log.Error(err)
					continue
				}
				if ok {
					// we were able to steal the lease, so start handling messages
					s.startReceiver(ctx, stolen)
				}
			}
		}
	}
}

func (s *scheduler) Stop() error {
	if s.done != nil {
		s.done()
	}

	// close all receivers even if errors occur reporting only the last error, but logging all
	var lastErr error
	for _, lr := range s.receivers {
		if err := lr.Close(); err != nil {
			log.Error(err)
			lastErr = err
		}
	}

	return lastErr
}

func (s *scheduler) startReceiver(ctx context.Context, lease *Lease) error {
	if receiver, ok := s.receivers[lease.partitionID]; ok {
		// receiver thinks it's already running... this is probably a bug if it happens
		if err := receiver.Close(); err != nil {
			log.Error(err)
		}
		delete(s.receivers, lease.partitionID)
	}
	lr := newLeasedReceiver(s.processor, lease)
	if err := lr.Run(ctx); err != nil {
		return err
	}
	s.receivers[lease.partitionID] = lr
	return nil
}

func (s *scheduler) stopReceiver(ctx context.Context, lease *Lease) error {
	if receiver, ok := s.receivers[lease.partitionID]; ok {
		err := receiver.Close()
		delete(s.receivers, lease.partitionID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *scheduler) acquireExpiredLeases(ctx context.Context, leases []*Lease) (acquired []*Lease, notAcquired []*Lease, err error) {
	for _, lease := range leases {
		if lease.IsExpired() {
			acquireCtx, cancel := context.WithTimeout(ctx, timeout)
			if acquiredLease, ok, err := s.processor.leaser.AcquireLease(acquireCtx, lease); ok {
				cancel()
				acquired = append(acquired, acquiredLease)
			} else {
				cancel()
				if err != nil {
					return nil, nil, err
				}
				notAcquired = append(notAcquired, lease)
			}
		} else {
			notAcquired = append(notAcquired, lease)
		}

	}
	return acquired, notAcquired, nil
}

func leaseToSteal(candidates []*Lease, myLeaseCount int) (*Lease, bool) {
	biggestOwner := ownerWithMostLeases(candidates)
	leasesByOwner := leasesByOwner(candidates)
	if (len(biggestOwner.Leases)-myLeaseCount) >= 2 && len(leasesByOwner[biggestOwner.Owner]) >= 1 {
		return leasesByOwner[biggestOwner.Owner][0], true
	}
	return nil, false
}

func ownerWithMostLeases(candidates []*Lease) *ownerCount {
	var largest *ownerCount
	for key, value := range leasesByOwner(candidates) {
		if largest == nil || len(largest.Leases) < len(value) {
			largest = &ownerCount{
				Owner:  key,
				Leases: value,
			}
		}
	}
	return largest
}

func leasesByOwner(candidates []*Lease) map[string][]*Lease {
	byOwner := make(map[string][]*Lease)
	for _, candidate := range candidates {
		if val, ok := byOwner[candidate.owner]; ok {
			byOwner[candidate.owner] = append(val, candidate)
		} else {
			byOwner[candidate.owner] = []*Lease{candidate}
		}
	}
	return byOwner
}
