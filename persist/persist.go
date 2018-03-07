package persist

import (
	"github.com/pkg/errors"
	"path"
	"sync"
)

type (
	// OffsetPersister provides persistence for the received offset for a given namespace, hub name, consumer group, partition Id and
	// offset so that if a receiver where to be interrupted, it could resume after the last consumed event.
	OffsetPersister interface {
		Write(namespace, name, consumerGroup, partitionID, offset string) error
		Read(namespace, name, consumerGroup, partitionID string) (string, error)
	}

	// MemoryPersister is a default implementation of a Hub OffsetPersister, which will persist offset information in
	// memory.
	MemoryPersister struct {
		values map[string]string
		mu     sync.Mutex
	}
)

func (p *MemoryPersister) Write(namespace, name, consumerGroup, partitionID, offset string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := getPersistenceKey(namespace, name, consumerGroup, partitionID)
	p.values[key] = offset
	return nil
}

func (p *MemoryPersister) Read(namespace, name, consumerGroup, partitionID string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := getPersistenceKey(namespace, name, consumerGroup, partitionID)
	if offset, ok := p.values[key]; ok {
		return offset, nil
	}
	return "", errors.Errorf("could not read the offset for the key %s", key)
}

func getPersistenceKey(namespace, name, consumerGroup, partitionID string) string {
	return path.Join(namespace, name, consumerGroup, partitionID)
}
