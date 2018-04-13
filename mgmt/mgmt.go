// Package mgmt provides functionality for calling the Event Hubs management operations
package mgmt

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
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-amqp-common-go/auth"
	"github.com/Azure/azure-amqp-common-go/rpc"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/mitchellh/mapstructure"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"pack.ag/amqp"
)

const (
	// MsftVendor is the Microsoft vendor identifier
	MsftVendor          = "com.microsoft"
	entityTypeKey       = "type"
	entityNameKey       = "name"
	partitionNameKey    = "partition"
	securityTokenKey    = "security_token"
	eventHubEntityType  = MsftVendor + ":eventhub"
	partitionEntityType = MsftVendor + ":partition"
	operationKey        = "operation"
	readOperationKey    = "READ"
	address             = "$management"
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
		Path           string    `mapstructure:"name"`
		CreatedAt      time.Time `mapstructure:"created_at"`
		PartitionCount int       `mapstructure:"partition_count"`
		PartitionIDs   []string  `mapstructure:"partition_ids"`
	}

	// HubPartitionRuntimeInformation provides management node information about a given Event Hub partition
	HubPartitionRuntimeInformation struct {
		HubPath                 string    `mapstructure:"name"`
		PartitionID             string    `mapstructure:"partition"`
		BeginningSequenceNumber int64     `mapstructure:"begin_sequence_number"`
		LastSequenceNumber      int64     `mapstructure:"last_enqueued_sequence_number"`
		LastEnqueuedOffset      string    `mapstructure:"last_enqueued_offset"`
		LastEnqueuedTimeUtc     time.Time `mapstructure:"last_enqueued_time_utc"`
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
	span, ctx := opentracing.StartSpanFromContext(ctx, "eventhub.mgmt.Client.GetHubRuntimeInformation")
	defer span.Finish()

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
	span, ctx := opentracing.StartSpanFromContext(ctx, "eventhub.mgmt.Client.GetHubPartitionRuntimeInformation")
	defer span.Finish()

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
	token, err := c.tokenProvider.GetToken(c.getTokenAudience())
	if err != nil {
		return nil, err
	}
	msg.ApplicationProperties[securityTokenKey] = token.Token

	return msg, nil
}

func (c *Client) getTokenAudience() string {
	return fmt.Sprintf("amqp://%s.%s/%s", c.namespace, c.env.ServiceBusEndpointSuffix, c.hubName)
}

func newHubPartitionRuntimeInformation(msg *amqp.Message) (*HubPartitionRuntimeInformation, error) {
	values, ok := msg.Value.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf("values were not map[string]interface{}, it was: %v", values)
	}

	var partitionInfo HubPartitionRuntimeInformation
	err := mapstructure.Decode(values, &partitionInfo)
	return &partitionInfo, err
}

// newHubRuntimeInformation constructs a new HubRuntimeInformation from an AMQP message
func newHubRuntimeInformation(msg *amqp.Message) (*HubRuntimeInformation, error) {
	values, ok := msg.Value.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf("values were not map[string]interface{}, it was: %v", values)
	}

	var runtimeInfo HubRuntimeInformation
	err := mapstructure.Decode(values, &runtimeInfo)
	return &runtimeInfo, err
}
