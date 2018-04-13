// Package storage provides implementations for Checkpointer and Leaser from package eph for persisting leases and
// checkpoints for the Event Processor Host using Azure Storage as a durable store.
package storage

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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/Azure/azure-amqp-common-go/log"
	"github.com/Azure/azure-amqp-common-go/persist"
	"github.com/Azure/azure-amqp-common-go/uuid"
	"github.com/Azure/azure-event-hubs-go"
	"github.com/Azure/azure-event-hubs-go/eph"
	"github.com/opentracing/opentracing-go"
	tag "github.com/opentracing/opentracing-go/ext"

	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/pkg/errors"
)

type (
	// LeaserCheckpointer implements the eph.LeaserCheckpointer interface for Azure Storage
	LeaserCheckpointer struct {
		leases            map[string]*storageLease
		processor         *eph.EventProcessorHost
		leaseDuration     time.Duration
		credential        Credential
		containerURL      *azblob.ContainerURL
		serviceURL        *azblob.ServiceURL
		containerName     string
		accountName       string
		env               azure.Environment
		dirtyPartitions   map[string]uuid.UUID
		updatedPartitions map[string]uuid.UUID
		leasesMu          sync.Mutex
		dirtyMu           sync.Mutex
		done              func()
	}

	storageLease struct {
		*eph.Lease
		leaser     *LeaserCheckpointer
		Checkpoint *persist.Checkpoint   `json:"checkpoint"`
		State      azblob.LeaseStateType `json:"state"`
		Token      string                `json:"token"`
	}

	// Credential is a wrapper for the Azure Storage azblob.Credential
	Credential interface {
		azblob.Credential
	}
)

// NewStorageLeaserCheckpointer builds an Azure Storage Leaser Checkpointer which handles leasing and checkpointing for
// the EventProcessorHost
func NewStorageLeaserCheckpointer(credential Credential, accountName, containerName string, env azure.Environment) (*LeaserCheckpointer, error) {
	storageURL, err := url.Parse("https://" + accountName + ".blob." + env.StorageEndpointSuffix)
	if err != nil {
		return nil, err
	}

	svURL := azblob.NewServiceURL(*storageURL, azblob.NewPipeline(credential, azblob.PipelineOptions{}))
	containerURL := svURL.NewContainerURL(containerName)

	return &LeaserCheckpointer{
		credential:        credential,
		containerName:     containerName,
		accountName:       accountName,
		leaseDuration:     eph.DefaultLeaseDuration,
		env:               env,
		serviceURL:        &svURL,
		containerURL:      &containerURL,
		leases:            make(map[string]*storageLease),
		dirtyPartitions:   make(map[string]uuid.UUID),
		updatedPartitions: make(map[string]uuid.UUID),
	}, nil
}

// SetEventHostProcessor sets the EventHostProcessor on the instance of the LeaserCheckpointer
func (sl *LeaserCheckpointer) SetEventHostProcessor(eph *eph.EventProcessorHost) {
	sl.processor = eph
	for _, partitionID := range eph.GetPartitionIDs() {
		sl.dirtyPartitions[partitionID] = uuid.Nil
		sl.updatedPartitions[partitionID] = uuid.Nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	go sl.persistLeases(ctx)
	sl.done = cancel
}

// StoreExists returns true if the storage container exists
func (sl *LeaserCheckpointer) StoreExists(ctx context.Context) (bool, error) {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.StoreExists")
	defer span.Finish()

	opts := azblob.ListContainersOptions{
		Prefix: sl.containerName,
	}
	res, err := sl.serviceURL.ListContainers(ctx, azblob.Marker{}, opts)
	if err != nil {
		return false, err
	}

	for _, container := range res.Containers {
		if container.Name == sl.containerName {
			return true, nil
		}
	}
	return false, nil
}

// EnsureStore creates the container if it does not exist
func (sl *LeaserCheckpointer) EnsureStore(ctx context.Context) error {
	sl.leasesMu.Lock()
	defer sl.leasesMu.Unlock()
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.EnsureStore")
	defer span.Finish()

	ok, err := sl.StoreExists(ctx)
	if err != nil {
		return err
	}

	if !ok {
		containerURL := sl.serviceURL.NewContainerURL(sl.containerName)
		_, err := containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
		if err != nil {
			return err
		}
		sl.containerURL = &containerURL
	}
	return nil
}

// DeleteStore deletes the Azure Storage container
func (sl *LeaserCheckpointer) DeleteStore(ctx context.Context) error {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.DeleteStore")
	defer span.Finish()
	_, err := sl.containerURL.Delete(ctx, azblob.ContainerAccessConditions{})
	return err
}

// GetLeases gets all of the partition leases
func (sl *LeaserCheckpointer) GetLeases(ctx context.Context) ([]eph.LeaseMarker, error) {
	sl.leasesMu.Lock()
	defer sl.leasesMu.Unlock()
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.GetLeases")
	defer span.Finish()

	partitionIDs := sl.processor.GetPartitionIDs()
	leases := make([]eph.LeaseMarker, len(partitionIDs))
	for idx, partitionID := range partitionIDs {
		lease, err := sl.getLease(ctx, partitionID)
		if err != nil {
			return nil, err
		}
		leases[idx] = lease
	}
	return leases, nil
}

// EnsureLease creates a lease in the container if it doesn't exist
func (sl *LeaserCheckpointer) EnsureLease(ctx context.Context, partitionID string) (eph.LeaseMarker, error) {
	sl.leasesMu.Lock()
	defer sl.leasesMu.Unlock()
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.EnsureLease")
	defer span.Finish()

	return sl.createOrGetLease(ctx, partitionID)
}

// DeleteLease deletes a lease in the storage container
func (sl *LeaserCheckpointer) DeleteLease(ctx context.Context, partitionID string) error {
	sl.leasesMu.Lock()
	defer sl.leasesMu.Unlock()
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.DeleteLease")
	defer span.Finish()

	_, err := sl.containerURL.NewBlobURL(partitionID).Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	delete(sl.leases, partitionID)
	return err
}

// AcquireLease acquires the lease to the Azure blob in the container
func (sl *LeaserCheckpointer) AcquireLease(ctx context.Context, partitionID string) (eph.LeaseMarker, bool, error) {
	sl.leasesMu.Lock()
	defer sl.leasesMu.Unlock()
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.AcquireLease")
	defer span.Finish()

	blobURL := sl.containerURL.NewBlobURL(partitionID)
	lease, err := sl.getLease(ctx, partitionID)
	if err != nil {
		log.For(ctx).Error(err)
		return nil, false, nil
	}

	res, err := blobURL.GetPropertiesAndMetadata(ctx, azblob.BlobAccessConditions{})
	if err != nil {
		log.For(ctx).Error(err)
		return nil, false, err
	}

	uuidToken, err := uuid.NewV4()
	if err != nil {
		log.For(ctx).Error(err)
		return nil, false, err
	}

	newToken := uuidToken.String()
	if res.LeaseState() == azblob.LeaseStateLeased {
		// is leased by someone else due to a race to acquire
		_, err := blobURL.ChangeLease(ctx, lease.Token, newToken, azblob.HTTPAccessConditions{})
		if err != nil {
			log.For(ctx).Error(err)
			return nil, false, err
		}
	} else {
		_, err = blobURL.AcquireLease(ctx, newToken, int32(sl.leaseDuration.Round(time.Second).Seconds()), azblob.HTTPAccessConditions{})
		if err != nil {
			log.For(ctx).Error(err)
			return nil, false, err
		}
	}

	lease.Token = newToken
	lease.Owner = sl.processor.GetName()
	lease.IncrementEpoch()
	err = sl.uploadLease(ctx, lease)
	if err != nil {
		return nil, false, err
	}
	sl.leases[partitionID] = lease
	return lease, true, nil
}

// RenewLease renews the lease to the Azure blob
func (sl *LeaserCheckpointer) RenewLease(ctx context.Context, partitionID string) (eph.LeaseMarker, bool, error) {
	sl.leasesMu.Lock()
	defer sl.leasesMu.Unlock()
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.RenewLease")
	defer span.Finish()

	blobURL := sl.containerURL.NewBlobURL(partitionID)
	lease, ok := sl.leases[partitionID]
	if !ok {
		return nil, false, errors.New("lease was not found")
	}

	_, err := blobURL.RenewLease(ctx, lease.Token, azblob.HTTPAccessConditions{})
	if err != nil {
		log.For(ctx).Error(err)
		return nil, false, err
	}
	return lease, true, nil
}

// ReleaseLease releases the lease to the blob in Azure storage
func (sl *LeaserCheckpointer) ReleaseLease(ctx context.Context, partitionID string) (bool, error) {
	sl.leasesMu.Lock()
	defer sl.leasesMu.Unlock()
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.ReleaseLease")
	defer span.Finish()

	blobURL := sl.containerURL.NewBlobURL(partitionID)
	lease, ok := sl.leases[partitionID]
	if !ok {
		return false, errors.New("lease was not found")
	}

	_, err := blobURL.ReleaseLease(ctx, lease.Token, azblob.HTTPAccessConditions{})
	if err != nil {
		log.For(ctx).Error(err)
		return false, err
	}
	delete(sl.leases, partitionID)
	return true, nil
}

// UpdateLease renews and uploads the latest lease to the blob store
func (sl *LeaserCheckpointer) UpdateLease(ctx context.Context, partitionID string) (eph.LeaseMarker, bool, error) {
	sl.leasesMu.Lock()
	defer sl.leasesMu.Unlock()

	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.UpdateLease")
	defer span.Finish()

	return sl.updateLease(ctx, partitionID)
}

func (sl *LeaserCheckpointer) updateLease(ctx context.Context, partitionID string) (eph.LeaseMarker, bool, error) {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.updateLease")
	defer span.Finish()

	blobURL := sl.containerURL.NewBlobURL(partitionID)
	lease, ok := sl.leases[partitionID]
	if !ok {
		return nil, false, errors.New("lease was not found")
	}

	_, err := blobURL.RenewLease(ctx, lease.Token, azblob.HTTPAccessConditions{})
	if err != nil {
		log.For(ctx).Error(err)
		return nil, false, err
	}

	if !ok {
		return nil, false, errors.New("could not renew lease when updating lease")
	}

	err = sl.uploadLease(ctx, lease)
	if err != nil {
		log.For(ctx).Error(err)
		return nil, false, err
	}

	return lease, true, nil
}

// GetCheckpoint returns the latest checkpoint for the partitionID.
func (sl *LeaserCheckpointer) GetCheckpoint(ctx context.Context, partitionID string) (persist.Checkpoint, bool) {
	sl.leasesMu.Lock()
	defer sl.leasesMu.Unlock()

	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.GetCheckpoint")
	defer span.Finish()

	lease, ok := sl.leases[partitionID]
	if ok {
		return *lease.Checkpoint, ok
	}
	return persist.NewCheckpointFromStartOfStream(), ok
}

// EnsureCheckpoint ensures a checkpoint exists for the lease
func (sl *LeaserCheckpointer) EnsureCheckpoint(ctx context.Context, partitionID string) (persist.Checkpoint, error) {
	sl.leasesMu.Lock()
	defer sl.leasesMu.Unlock()

	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.EnsureCheckpoint")
	defer span.Finish()

	lease, ok := sl.leases[partitionID]
	if ok {
		if lease.Checkpoint == nil {
			checkpoint := persist.NewCheckpointFromStartOfStream()
			lease.Checkpoint = &checkpoint
		}
		return *lease.Checkpoint, nil
	}
	return persist.NewCheckpointFromStartOfStream(), nil
}

// UpdateCheckpoint will attempt to write the checkpoint to Azure Storage
func (sl *LeaserCheckpointer) UpdateCheckpoint(ctx context.Context, partitionID string, checkpoint persist.Checkpoint) error {
	sl.leasesMu.Lock()
	defer sl.leasesMu.Unlock()

	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.UpdateCheckpoint")
	defer span.Finish()

	lease, ok := sl.leases[partitionID]
	if !ok {
		return errors.New("lease for partition isn't owned by this EventProcessorHost")
	}

	lease.Checkpoint = &checkpoint
	dirtyPartitionID, err := uuid.NewV4()
	if err != nil {
		return err
	}
	sl.dirtyPartitions[partitionID] = dirtyPartitionID
	return nil
}

// DeleteCheckpoint will attempt to delete the checkpoint from Azure Storage
func (sl *LeaserCheckpointer) DeleteCheckpoint(ctx context.Context, partitionID string) error {
	sl.leasesMu.Lock()
	defer sl.leasesMu.Unlock()

	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.DeleteCheckpoint")
	defer span.Finish()

	lease, ok := sl.leases[partitionID]
	if !ok {
		return errors.New("lease for partition isn't owned by this EventProcessorHost")
	}

	checkpoint := persist.NewCheckpointFromStartOfStream()
	lease.Checkpoint = &checkpoint
	updatedLease, ok, err := sl.updateLease(ctx, lease.PartitionID)
	if err != nil {
		return err
	}

	if !ok {
		return errors.New("checkpoint update was not successful")
	}
	sl.leases[partitionID] = updatedLease.(*storageLease)
	return nil

}

// Close will stop the leaser / checkpointer from persisting dirty leases & checkpoints to storage
func (sl *LeaserCheckpointer) Close() error {
	if sl.done != nil {
		sl.done()
	}
	return nil
}

func (sl *LeaserCheckpointer) persistLeases(ctx context.Context) {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.persistLeases")
	defer span.Finish()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			for partitionID, val := range sl.dirtyPartitions {
				if val != sl.updatedPartitions[partitionID] {
					err := sl.persistLease(ctx, partitionID)
					if err != nil {
						continue
					}
					sl.updatedPartitions[partitionID] = val
				}
			}
			<-time.After(1 * time.Second)
		}
	}
}

func (sl *LeaserCheckpointer) persistLease(ctx context.Context, partitionID string) error {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.persistLease")
	defer span.Finish()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, ok, err := sl.UpdateLease(ctx, partitionID)
	cancel()
	if err != nil {
		return err
	}

	if !ok {
		return errors.New("unable to update dirty lease -- this may mean there will be reprocessing")
	}
	return nil
}

func (sl *LeaserCheckpointer) uploadLease(ctx context.Context, lease *storageLease) error {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.uploadLease")
	defer span.Finish()

	blobURL := sl.containerURL.NewBlobURL(lease.PartitionID)
	jsonLease, err := json.Marshal(lease)
	if err != nil {
		return err
	}
	reader := bytes.NewReader(jsonLease)
	_, err = blobURL.ToBlockBlobURL().PutBlob(ctx, reader, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{
		LeaseAccessConditions: azblob.LeaseAccessConditions{
			LeaseID: lease.Token,
		},
	})

	return err
}

func (sl *LeaserCheckpointer) createOrGetLease(ctx context.Context, partitionID string) (*storageLease, error) {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.createOrGetLease")
	defer span.Finish()

	lease := &storageLease{
		Lease: &eph.Lease{
			PartitionID: partitionID,
		},
	}
	blobURL := sl.containerURL.NewBlobURL(partitionID)
	jsonLease, err := json.Marshal(lease)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(jsonLease)
	res, err := blobURL.ToBlockBlobURL().PutBlob(ctx, reader, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{
		HTTPAccessConditions: azblob.HTTPAccessConditions{
			IfNoneMatch: "*",
		},
	})

	if err != nil {
		return nil, err
	}

	if res.StatusCode() == 404 {
		return sl.getLease(ctx, partitionID)
	}
	return lease, err
}

func (sl *LeaserCheckpointer) getLease(ctx context.Context, partitionID string) (*storageLease, error) {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.LeaserCheckpointer.getLease")
	defer span.Finish()

	blobURL := sl.containerURL.NewBlobURL(partitionID)
	res, err := blobURL.GetBlob(ctx, azblob.BlobRange{}, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, err
	}
	return sl.leaseFromResponse(res)
}

func (sl *LeaserCheckpointer) leaseFromResponse(res *azblob.GetResponse) (*storageLease, error) {
	buf := new(bytes.Buffer)
	buf.ReadFrom(res.Response().Body)
	var lease storageLease
	if err := json.Unmarshal(buf.Bytes(), &lease); err != nil {
		return nil, err
	}
	lease.leaser = sl
	lease.State = res.LeaseState()
	return &lease, nil
}

func (sl *LeaserCheckpointer) dlog(ctx context.Context, msg string) {
	name := sl.processor.GetName()
	log.For(ctx).Debug(fmt.Sprintf("storage leaser eph %q: "+msg, name))
}

// IsExpired checks to see if the blob is not still leased
func (s *storageLease) IsExpired(ctx context.Context) bool {
	span, ctx := startConsumerSpanFromContext(ctx, "eventhub.storage.storageLease.IsExpired")
	defer span.Finish()

	lease, err := s.leaser.getLease(ctx, s.PartitionID)
	if err != nil {
		return false
	}
	return lease.State != azblob.LeaseStateLeased
}

func (s *storageLease) String() string {
	bits, err := json.Marshal(s)
	if err != nil {
		return ""
	}
	return string(bits)
}

func startConsumerSpanFromContext(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
	eventhub.ApplyComponentInfo(span)
	tag.SpanKindRPCClient.Set(span)
	span.SetTag("eventhub.eventprocessorhost.kind", "azure.storage")
	return span, ctx
}
