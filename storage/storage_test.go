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
	"context"
	"strings"
	"time"

	"github.com/Azure/azure-amqp-common-go/aad"
	"github.com/Azure/azure-event-hubs-go/eph"
	"github.com/Azure/azure-event-hubs-go/internal/test"
	mgmt "github.com/Azure/azure-sdk-for-go/services/eventhub/mgmt/2017-04-01/eventhub"
	"github.com/stretchr/testify/assert"
)

func (ts *testSuite) TestLeaserStoreCreation() {
	leaser, del := ts.newLeaser()
	defer del()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	exists, err := leaser.StoreExists(ctx)
	if err != nil {
		ts.T().Error(err)
	}
	assert.False(ts.T(), exists)

	err = leaser.EnsureStore(ctx)
	if err != nil {
		ts.T().Error(err)
	}

	exists, err = leaser.StoreExists(ctx)
	if err != nil {
		ts.T().Error(err)
	}
	assert.True(ts.T(), exists)
}

func (ts *testSuite) TestLeaserLeaseEnsure() {
	leaser, del := ts.leaserWithEPH()
	defer del()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, partitionID := range leaser.processor.GetPartitionIDs() {
		lease, err := leaser.EnsureLease(ctx, partitionID)
		if err != nil {
			ts.T().Error(err)
		}
		assert.Equal(ts.T(), partitionID, lease.GetPartitionID())
	}
}

func (ts *testSuite) TestLeaserAcquire() {
	leaser, del := ts.leaserWithEPHAndLeases()
	defer del()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leases, err := leaser.GetLeases(ctx)
	if err != nil {
		ts.T().Error(err)
	}
	assert.Equal(ts.T(), len(leaser.processor.GetPartitionIDs()), len(leases))

	for _, lease := range leases {
		epochBefore := lease.GetEpoch()
		acquiredLease, ok, err := leaser.AcquireLease(ctx, lease.GetPartitionID())
		if err != nil {
			ts.T().Error(err)
			break
		}
		if !ok {
			assert.Fail(ts.T(), "should have acquired the lease")
			break
		}
		assert.Equal(ts.T(), epochBefore+1, acquiredLease.GetEpoch())
		assert.Equal(ts.T(), leaser.processor.GetName(), acquiredLease.GetOwner())
		assert.NotNil(ts.T(), acquiredLease.(*storageLease).Token)
	}
	assert.Equal(ts.T(), len(leaser.processor.GetPartitionIDs()), len(leaser.leases))
}

func (ts *testSuite) TestLeaserRenewLease() {
	leaser, del := ts.leaserWithEPHAndLeases()
	defer del()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leases, err := leaser.GetLeases(ctx)
	if err != nil {
		ts.T().Error(err)
	}

	lease := leases[0]
	// should fail if lease was never acquired
	_, ok, err := leaser.RenewLease(ctx, lease.GetPartitionID())
	assert.NotNil(ts.T(), err)
	assert.False(ts.T(), ok, "shouldn't be ok")

	acquired, ok, err := leaser.AcquireLease(ctx, lease.GetPartitionID())
	assert.Nil(ts.T(), err)
	if !ok {
		assert.FailNow(ts.T(), "wasn't able to acquire lease")
	}

	_, ok, err = leaser.RenewLease(ctx, acquired.GetPartitionID())
	assert.Nil(ts.T(), err)
	assert.True(ts.T(), ok, "should have acquired")
}

func (ts *testSuite) TestLeaserRelease() {
	leaser, del := ts.leaserWithEPHAndLeases()
	defer del()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leases, err := leaser.GetLeases(ctx)
	if err != nil {
		ts.T().Error(err)
	}

	lease := leases[0]
	acquired, ok, err := leaser.AcquireLease(ctx, lease.GetPartitionID())
	assert.Nil(ts.T(), err)
	assert.True(ts.T(), ok, "should have acquired")
	assert.Equal(ts.T(), 1, len(leaser.leases))

	ok, err = leaser.ReleaseLease(ctx, acquired.GetPartitionID())
	assert.Nil(ts.T(), err)
	assert.True(ts.T(), ok, "should have released")
	assert.Equal(ts.T(), 0, len(leaser.leases))
}

func (ts *testSuite) leaserWithEPHAndLeases() (*LeaserCheckpointer, func()) {
	leaser, del := ts.leaserWithEPH()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, partitionID := range leaser.processor.GetPartitionIDs() {
		lease, err := leaser.EnsureLease(ctx, partitionID)
		if err != nil {
			ts.T().Error(err)
		}
		assert.Equal(ts.T(), partitionID, lease.GetPartitionID())
	}

	return leaser, del
}

func (ts *testSuite) leaserWithEPH() (*LeaserCheckpointer, func()) {
	leaser, del := ts.newLeaser()
	hub, delHub := ts.ensureRandomHub("stortest", 4)

	delAll := func() {
		delHub()
		del()
	}

	provider, err := aad.NewJWTProvider(aad.JWTProviderWithEnvironmentVars())
	if err != nil {
		ts.T().Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	processor, err := eph.New(ctx, ts.Namespace, *hub.Name, provider, nil, nil)
	leaser.SetEventHostProcessor(processor)

	err = leaser.EnsureStore(ctx)
	if err != nil {
		ts.T().Fatal(err)
	}

	return leaser, delAll
}

func (ts *testSuite) newLeaser() (*LeaserCheckpointer, func()) {
	containerName := strings.ToLower(ts.RandomName("stortest", 4))
	cred, err := NewAADSASCredential(ts.SubscriptionID, test.ResourceGroupName, ts.AccountName, containerName, AADSASCredentialWithEnvironmentVars())
	if err != nil {
		ts.T().Fatal(err)
	}

	leaser, err := NewStorageLeaserCheckpointer(cred, ts.AccountName, containerName, ts.Env)
	if err != nil {
		ts.T().Fatal(err)
	}

	return leaser, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := leaser.DeleteStore(ctx); err != nil {
			ts.T().Fatal(err)
		}
	}
}

func (ts *testSuite) ensureRandomHubByName(hubName string) (*mgmt.Model, func()) {
	hub, err := ts.EnsureEventHub(context.Background(), hubName)
	if err != nil {
		ts.T().Fatal(err)
	}

	return hub, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := ts.DeleteEventHub(ctx, hubName)
		if err != nil {
			ts.T().Fatal(err)
		}
	}
}

func (ts *testSuite) ensureRandomHub(prefix string, length int) (*mgmt.Model, func()) {
	return ts.ensureRandomHubByName(ts.RandomName(prefix, length))
}
