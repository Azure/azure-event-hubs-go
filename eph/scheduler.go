package eph

import (
	"context"
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	timeout = 60 * time.Second
)

const (
	// DefaultLeaseRenewalInterval defines the default amount of time between lease renewal attempts
	DefaultLeaseRenewalInterval = 10 * time.Second

	// DefaultLeaseDurationInSeconds defines the default amount of time a lease is valid
	DefaultLeaseDurationInSeconds = 30 * time.Second
)

type (
	scheduler struct {
		processor            *EventProcessorHost
		receivers            map[string]*leasedReceiver
		done                 func()
		leaseRenewalInterval time.Duration
	}

	ownerCount struct {
		Owner  string
		Leases []LeaseMarker
	}
)

func newScheduler(eventHostProcessor *EventProcessorHost) *scheduler {
	return &scheduler{
		processor:            eventHostProcessor,
		receivers:            make(map[string]*leasedReceiver),
		leaseRenewalInterval: DefaultLeaseRenewalInterval,
	}
}

func (s *scheduler) Run() {
	ctx, done := context.WithCancel(context.Background())
	s.done = done
	for {
		select {
		case <-ctx.Done():
			return
		default:
			log.Debugf("running scan")
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
			log.Debugf("acquired: %v, not acquired: %v", acquired, notAcquired)
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
			var leasesOwnedByOthers []LeaseMarker
			for key, value := range byOwner {
				if key != s.processor.name {
					leasesOwnedByOthers = append(leasesOwnedByOthers, value...)
				}
			}

			// try to steal work away from others if work has become imbalanced
			if candidate, ok := leaseToSteal(leasesOwnedByOthers, countOwnedByMe); ok {
				acquireCtx, cancel := context.WithTimeout(ctx, timeout)
				stolen, ok, err := s.processor.leaser.AcquireLease(acquireCtx, candidate.GetPartitionID())
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
			skew := time.Duration(rand.Intn(1000)-500) * time.Millisecond
			time.Sleep(s.leaseRenewalInterval + skew)
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

func (s *scheduler) startReceiver(ctx context.Context, lease LeaseMarker) error {
	if receiver, ok := s.receivers[lease.GetPartitionID()]; ok {
		// receiver thinks it's already running... this is probably a bug if it happens
		if err := receiver.Close(); err != nil {
			log.Error(err)
		}
		delete(s.receivers, lease.GetPartitionID())
	}
	lr := newLeasedReceiver(s.processor, lease)
	if err := lr.Run(ctx); err != nil {
		return err
	}
	s.receivers[lease.GetPartitionID()] = lr
	return nil
}

func (s *scheduler) stopReceiver(ctx context.Context, lease LeaseMarker) error {
	log.Debugf("stopping receiver for partitionID %q", lease.GetPartitionID())
	if receiver, ok := s.receivers[lease.GetPartitionID()]; ok {
		err := receiver.Close()
		delete(s.receivers, lease.GetPartitionID())
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *scheduler) acquireExpiredLeases(ctx context.Context, leases []LeaseMarker) (acquired []LeaseMarker, notAcquired []LeaseMarker, err error) {
	for _, lease := range leases {
		if lease.IsExpired(ctx) {
			acquireCtx, cancel := context.WithTimeout(ctx, timeout)
			if acquiredLease, ok, err := s.processor.leaser.AcquireLease(acquireCtx, lease.GetPartitionID()); ok {
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

func leaseToSteal(candidates []LeaseMarker, myLeaseCount int) (LeaseMarker, bool) {
	biggestOwner := ownerWithMostLeases(candidates)
	leasesByOwner := leasesByOwner(candidates)
	if biggestOwner != nil && leasesByOwner[biggestOwner.Owner] != nil &&
		(len(biggestOwner.Leases)-myLeaseCount) >= 2 && len(leasesByOwner[biggestOwner.Owner]) >= 1 {
		return leasesByOwner[biggestOwner.Owner][0], true
	}
	return nil, false
}

func ownerWithMostLeases(candidates []LeaseMarker) *ownerCount {
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

func leasesByOwner(candidates []LeaseMarker) map[string][]LeaseMarker {
	byOwner := make(map[string][]LeaseMarker)
	for _, candidate := range candidates {
		if val, ok := byOwner[candidate.GetOwner()]; ok {
			byOwner[candidate.GetOwner()] = append(val, candidate)
		} else {
			byOwner[candidate.GetOwner()] = []LeaseMarker{candidate}
		}
	}
	return byOwner
}
