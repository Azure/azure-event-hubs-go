package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/Azure/azure-event-hubs-go/auth"
	"github.com/Azure/azure-event-hubs-go/eph"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

type (
	// Leaser implements the eph.Leaser interface for Azure Storage
	Leaser struct {
		leases        map[string]*storageLease
		processor     *eph.EventProcessorHost
		leaseDuration time.Duration
		tokenProvider auth.TokenProvider
		containerURL  *azblob.ContainerURL
		serviceURL    *azblob.ServiceURL
		containerName string
	}

	storageLease struct {
		*eph.Lease
		leaser     *Leaser
		Checkpoint *eph.Checkpoint
		State      azblob.LeaseStateType
		Token      string `json:"token"`
	}
)

// NewStorageLeaser builds a Leaser which uses Azure Storage to store partition leases
func NewStorageLeaser(tokenProvider auth.TokenProvider, accountName, containerName string, leaseDuration time.Duration) *Leaser {
	return &Leaser{
		tokenProvider: tokenProvider,
		leaseDuration: eph.DefaultLeaseDurationInSeconds,
	}
}

// SetEventHostProcessor sets the EventHostProcessor on the instance of the Leaser
func (sl *Leaser) SetEventHostProcessor(eph *eph.EventProcessorHost) {
	sl.processor = eph
}

// StoreExists returns true if the storage container exists
func (sl *Leaser) StoreExists(ctx context.Context) (bool, error) {
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
func (sl *Leaser) EnsureStore(ctx context.Context) error {
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
func (sl *Leaser) DeleteStore(ctx context.Context) error {
	_, err := sl.containerURL.Delete(ctx, azblob.ContainerAccessConditions{})
	return err
}

// GetLeases gets all of the partition leases
func (sl *Leaser) GetLeases(ctx context.Context) ([]eph.LeaseMarker, error) {
	partitionIDs := sl.processor.GetPartitionIDs()
	leases := make([]eph.LeaseMarker, len(partitionIDs))
	for idx, partitionID := range partitionIDs {
		lease, err := sl.getLease(ctx, partitionID)
		if err != nil {
			return nil, err
		}
		leases[idx] = lease
	}
	return nil, nil
}

// EnsureLease creates a lease in the container if it doesn't exist
func (sl *Leaser) EnsureLease(ctx context.Context, partitionID string) (eph.LeaseMarker, error) {
	return sl.createOrGetLease(ctx, partitionID)
}

// DeleteLease deletes a lease in the storage container
func (sl *Leaser) DeleteLease(ctx context.Context, partitionID string) error {
	_, err := sl.containerURL.NewBlobURL(partitionID).Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	return err
}

// AcquireLease acquires the lease to the Azure blob in the container
func (sl *Leaser) AcquireLease(ctx context.Context, partitionID string) (eph.LeaseMarker, bool, error) {
	blobURL := sl.containerURL.NewBlobURL(partitionID)
	lease, err := sl.getLease(ctx, partitionID)
	if err != nil {
		return nil, false, nil
	}

	res, err := blobURL.GetPropertiesAndMetadata(ctx, azblob.BlobAccessConditions{})
	if err != nil {
		return nil, false, err
	}

	if res.LeaseState() == azblob.LeaseStateLeased {
		// is leased by someone else due to a race to acquire
		return nil, false, nil
	}

	newToken := uuid.NewV4().String()
	_, err = blobURL.AcquireLease(ctx, newToken, int32(sl.leaseDuration*time.Second), azblob.HTTPAccessConditions{})
	if err != nil {
		return nil, false, err
	}

	lease.Token = newToken
	lease.Owner = sl.processor.GetName()
	lease.IncrementEpoch()
	err = sl.uploadLease(ctx, lease)
	if err != nil {
		return nil, false, err
	}
	return lease, true, nil
}

// RenewLease renews the lease to the Azure blob
func (sl *Leaser) RenewLease(ctx context.Context, partitionID string) (eph.LeaseMarker, bool, error) {
	blobURL := sl.containerURL.NewBlobURL(partitionID)
	lease, ok := sl.leases[partitionID]
	if !ok {
		return nil, false, errors.New("lease was not found")
	}

	_, err := blobURL.RenewLease(ctx, lease.Token, azblob.HTTPAccessConditions{})
	if err != nil {
		return nil, false, err
	}
	return lease, true, nil
}

// ReleaseLease releases the lease to the blob in Azure storage
func (sl *Leaser) ReleaseLease(ctx context.Context, partitionID string) (bool, error) {
	blobURL := sl.containerURL.NewBlobURL(partitionID)
	lease, ok := sl.leases[partitionID]
	if !ok {
		return false, errors.New("lease was not found")
	}

	_, err := blobURL.RenewLease(ctx, lease.Token, azblob.HTTPAccessConditions{})
	if err != nil {
		return false, err
	}
	return true, nil
}

// UpdateLease renews and uploads the latest lease to the blob store
func (sl *Leaser) UpdateLease(ctx context.Context, partitionID string) (eph.LeaseMarker, bool, error) {
	lease, ok, err := sl.RenewLease(ctx, partitionID)
	if err != nil {
		return nil, false, err
	}

	if !ok {
		return nil, false, errors.New("could not renew lease when updating lease")
	}

	err = sl.uploadLease(ctx, lease.(*storageLease))
	if err != nil {
		return nil, false, err
	}

	return lease, true, nil
}

func (sl *Leaser) uploadLease(ctx context.Context, lease *storageLease) error {
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

	if err != nil {
		return err
	}
	return nil
}

func (sl *Leaser) createOrGetLease(ctx context.Context, partitionID string) (*storageLease, error) {
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

func (sl *Leaser) getLease(ctx context.Context, partitionID string) (*storageLease, error) {
	blobURL := sl.containerURL.NewBlobURL(partitionID)
	res, err := blobURL.GetBlob(ctx, azblob.BlobRange{}, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, err
	}
	checkpoint, err := leaseFromResponse(res)
	if err != nil {
		return nil, err
	}

	return &storageLease{
		Checkpoint: checkpoint,
		State:      res.LeaseState(),
	}, nil
}

func leaseFromResponse(res *azblob.GetResponse) (*eph.Checkpoint, error) {
	buf := new(bytes.Buffer)
	buf.ReadFrom(res.Response().Body)
	var checkpoint eph.Checkpoint
	if err := json.Unmarshal(buf.Bytes(), &checkpoint); err != nil {
		return nil, err
	}
	return &checkpoint, nil
}

// IsExpired checks to see if the blob is not still leased
func (s *storageLease) IsExpired(ctx context.Context) bool {
	lease, err := s.leaser.getLease(ctx, s.Checkpoint.PartitionID)
	if err != nil {
		return false
	}
	return lease.State != azblob.LeaseStateLeased
}
