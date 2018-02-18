package mgmt

import ()

const (
	// MsftVendor is the Microsoft vendor identifier
	MsftVendor = "com.microsoft"
	// Address is the management AMQP node address
	Address = "$management"

	// EventHubEntityType is the Event Hub entity type in AMQP
	EventHubEntityType = MsftVendor + ":eventhub"
	// PartitionEntityType is the Event Hub partition entity type in AMQP
	PartitionEntityType = MsftVendor + ":partition"

	// OperationKey is the map key used to specify the management operation
	OperationKey = "operation"
	// ReadOperationKey is the map key used to specify a read operation
	ReadOperationKey = "READ"
)
