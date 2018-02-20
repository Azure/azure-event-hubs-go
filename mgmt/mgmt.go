package mgmt

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-event-hubs-go/auth"
	"github.com/Azure/azure-event-hubs-go/rpc"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/pkg/errors"
	"pack.ag/amqp"
)

const (
	// MsftVendor is the Microsoft vendor identifier
	MsftVendor = "com.microsoft"

	entityTypeKey    = "type"
	entityNameKey    = "name"
	partitionNameKey = "partition"
	securityTokenKey = "security_token"

	resultCreatedAtKey      = "created_at"
	resultPartitionIDsKey   = "partition_ids"
	resultPartitionCountKey = "partition_count"

	resultBeginSequenceNumKey        = "begin_sequence_number"
	resultLastEnqueuedSequenceNumKey = "last_enqueued_sequence_number"
	resultLastEnqueuedOffsetKey      = "last_enqueued_offset"
	resultLastEnqueueTimeUtcKey      = "last_enqueued_time_utc"

	// eventHubEntityType is the Event Hub entity type in AMQP
	eventHubEntityType = MsftVendor + ":eventhub"
	// partitionEntityType is the Event Hub partition entity type in AMQP
	partitionEntityType = MsftVendor + ":partition"

	// operationKey is the map key used to specify the management operation
	operationKey = "operation"
	// readOperationKey is the map key used to specify a read operation
	readOperationKey = "READ"
	address          = "$management"
)

type (
	// Client communicates with an AMQP management node
	Client struct {
		namespace     string
		hubName       string
		tokenProvider auth.TokenProvider
		env           azure.Environment
	}

	// HubRuntimeInformation provides management node information about a given Event Hub instance
	HubRuntimeInformation struct {
		// Path is the name of the hub
		Path string
		// CreatedAt is the date and time the hub was created in UTC
		CreatedAt time.Time
		// PartitionCount is the number of partitions in the hub
		PartitionCount int
		// PartitionIDs is the slice of string partition identifiers
		PartitionIDs []string
	}

	// HubPartitionRuntimeInformation provides management node information about a given Event Hub partition
	HubPartitionRuntimeInformation struct {
		// HubPath is the name of the hub
		HubPath string
		// PartitionID is the identifier for the partition
		PartitionID string
		// BeginningSequenceNumber is the starting sequence number for the partition's message log
		BeginningSequenceNumber int64
		// LastSequenceNumber is the ending sequence number for the partition's message log
		LastSequenceNumber int64
		// LastEnqueuedOffset is the offset of the last enqueued message in the partition's message log
		LastEnqueuedOffset string
		// LastEnqueuedTimeUtc is the time of the last enqueued message in the partition's message log in UTC
		LastEnqueuedTimeUtc time.Time
	}
)

// NewClient constructs a new AMQP management client
func NewClient(namespace, hubName string, provider auth.TokenProvider, env azure.Environment) *Client {
	return &Client{
		namespace:     namespace,
		hubName:       hubName,
		tokenProvider: provider,
		env:           env,
	}
}

// GetHubRuntimeInformation requests runtime information for an Event Hub
func (c *Client) GetHubRuntimeInformation(ctx context.Context, conn *amqp.Client) (*HubRuntimeInformation, error) {
	rpcLink, err := rpc.NewLink(conn, address)
	if err != nil {
		return nil, err
	}

	msg := &amqp.Message{
		ApplicationProperties: map[string]interface{}{
			operationKey:  readOperationKey,
			entityTypeKey: eventHubEntityType,
			entityNameKey: c.hubName,
		},
	}
	msg, err = c.addSecurityToken(msg)
	if err != nil {
		return nil, err
	}

	res, err := rpcLink.RetryableRPC(ctx, 3, 1*time.Second, msg)
	if err != nil {
		return nil, err
	}

	hubRuntimeInfo, err := newHubRuntimeInformation(res.Message)
	if err != nil {
		return nil, err
	}
	return hubRuntimeInfo, nil
}

// GetHubPartitionRuntimeInformation fetches runtime information from the AMQP management node for a given partition
func (c *Client) GetHubPartitionRuntimeInformation(ctx context.Context, conn *amqp.Client, partitionID string) (*HubPartitionRuntimeInformation, error) {
	rpcLink, err := rpc.NewLink(conn, address)
	if err != nil {
		return nil, err
	}

	msg := &amqp.Message{
		ApplicationProperties: map[string]interface{}{
			operationKey:     readOperationKey,
			entityTypeKey:    partitionEntityType,
			entityNameKey:    c.hubName,
			partitionNameKey: partitionID,
		},
	}
	msg, err = c.addSecurityToken(msg)
	if err != nil {
		return nil, err
	}

	res, err := rpcLink.RetryableRPC(ctx, 3, 1*time.Second, msg)
	if err != nil {
		return nil, err
	}

	hubPartitionRuntimeInfo, err := newHubPartitionRuntimeInformation(res.Message)
	if err != nil {
		return nil, err
	}
	return hubPartitionRuntimeInfo, nil
}

func (c *Client) addSecurityToken(msg *amqp.Message) (*amqp.Message, error) {
	// TODO (devigned): need to uncomment this functionality after getting some guidance from the Event Hubs team (only works for SAS tokens right now)

	//token, err := c.tokenProvider.GetToken(c.getTokenAudience())
	//if err != nil {
	//	return nil, err
	//}
	//msg.ApplicationProperties[securityTokenKey] = token.Token

	return msg, nil
}

func (c *Client) getTokenAudience() string {
	return fmt.Sprintf("amqp://%s.%s/%s", c.namespace, c.env.ServiceBusEndpointSuffix, c.hubName)
}

func newHubPartitionRuntimeInformation(msg *amqp.Message) (*HubPartitionRuntimeInformation, error) {
	const errMsgFmt = "could not read %q key from message when creating a new hub partition runtime information -- message value: %v"
	info := new(HubPartitionRuntimeInformation)
	values, ok := msg.Value.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf("values were not map[string]interface{}, it was: %v", values)
	}

	if hubPath, ok := values[entityNameKey].(string); ok {
		info.HubPath = hubPath
	} else {
		return nil, errors.Errorf(errMsgFmt, entityNameKey, values)
	}

	if partition, ok := values[partitionNameKey].(string); ok {
		info.PartitionID = partition
	} else {
		return nil, errors.Errorf(errMsgFmt, partitionNameKey, values)
	}

	if sequence, ok := values[resultBeginSequenceNumKey].(int64); ok {
		info.BeginningSequenceNumber = sequence
	} else {
		return nil, errors.Errorf(errMsgFmt, resultBeginSequenceNumKey, values)
	}

	if sequence, ok := values[resultLastEnqueuedSequenceNumKey].(int64); ok {
		info.LastSequenceNumber = sequence
	} else {
		return nil, errors.Errorf(errMsgFmt, resultLastEnqueuedSequenceNumKey, values)
	}

	if lastOffset, ok := values[resultLastEnqueuedOffsetKey].(string); ok {
		info.LastEnqueuedOffset = lastOffset
	} else {
		return nil, errors.Errorf(errMsgFmt, resultLastEnqueuedOffsetKey, values)
	}

	if t, ok := values[resultLastEnqueueTimeUtcKey].(time.Time); ok {
		info.LastEnqueuedTimeUtc = t
	} else {
		return nil, errors.Errorf(errMsgFmt, resultLastEnqueueTimeUtcKey, values)
	}

	return info, nil
}

// newHubRuntimeInformation constructs a new HubRuntimeInformation from an AMQP message
func newHubRuntimeInformation(msg *amqp.Message) (*HubRuntimeInformation, error) {
	const errMsgFmt = "could not read %q key from message when creating a new hub runtime information -- message value: %v"
	info := new(HubRuntimeInformation)
	values, ok := msg.Value.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf("values were not map[string]interface{}, it was: %v", values)
	}

	if path, ok := values[entityNameKey].(string); ok {
		info.Path = path
	} else {
		return nil, errors.Errorf(errMsgFmt, entityNameKey, values)
	}

	if createdAt, ok := values[resultCreatedAtKey].(time.Time); ok {
		info.CreatedAt = createdAt
	} else {
		return nil, errors.Errorf(errMsgFmt, resultCreatedAtKey, values)
	}

	if count, ok := values[resultPartitionCountKey].(int32); ok {
		info.PartitionCount = int(count)
	} else {
		return nil, errors.Errorf(errMsgFmt, resultPartitionCountKey, values)
	}

	if ids, ok := values[resultPartitionIDsKey].([]string); ok {
		info.PartitionIDs = ids
	} else {
		return nil, errors.Errorf(errMsgFmt, resultPartitionCountKey, values)
	}

	return info, nil
}
