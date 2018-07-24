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
	"errors"
	"log"
	"net/url"
	"strings"
	"testing"

	"github.com/Azure/azure-amqp-common-go"
	"github.com/Azure/azure-event-hubs-go/internal/test"
	"github.com/Azure/azure-sdk-for-go/services/storage/mgmt/2017-10-01/storage"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/Azure/go-autorest/autorest/azure"
	azauth "github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/stretchr/testify/suite"
)

type (
	// eventHubSuite encapsulates a end to end test of Event Hubs with build up and tear down of all EH resources
	testSuite struct {
		test.BaseSuite
		AccountName string
		ServiceURL  *azblob.ServiceURL
	}
)

func TestStorage(t *testing.T) {
	suite.Run(t, new(testSuite))
}

func (ts *testSuite) SetupSuite() {
	ts.BaseSuite.SetupSuite()
	ts.AccountName = strings.ToLower(test.RandomString("ehtest", 6))
	ts.Require().NoError(ts.ensureStorageAccount())
}

func (ts *testSuite) TearDownSuite() {
	ts.BaseSuite.TearDownSuite()
	err := ts.deleteStorageAccount()
	if err != nil {
		ts.T().Error(err)
	}
}

func (ts *testSuite) TestCredential() {
	containerName := "foo"
	blobName := "bar"
	message := "Hello World!!"
	tokenProvider, err := NewAADSASCredential(ts.SubscriptionID, test.ResourceGroupName, ts.AccountName, containerName, AADSASCredentialWithEnvironmentVars())
	if err != nil {
		ts.T().Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
	defer cancel()
	pipeline := azblob.NewPipeline(tokenProvider, azblob.PipelineOptions{})
	fooURL, err := url.Parse("https://" + ts.AccountName + ".blob." + ts.Env.StorageEndpointSuffix + "/" + containerName)
	if err != nil {
		ts.T().Error(err)
	}

	containerURL := azblob.NewContainerURL(*fooURL, pipeline)
	defer containerURL.Delete(ctx, azblob.ContainerAccessConditions{})
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		ts.T().Error(err)
	}

	blobURL := containerURL.NewBlobURL(blobName).ToBlockBlobURL()
	_, err = blobURL.PutBlob(ctx, strings.NewReader(message), azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	if err != nil {
		ts.T().Error(err)
	}
}

func (ts *testSuite) deleteStorageAccount() error {
	ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
	defer cancel()

	client := getStorageAccountMgmtClient(ts.SubscriptionID, ts.Env)
	_, err := client.Delete(ctx, test.ResourceGroupName, ts.AccountName)
	return err
}

func (ts *testSuite) ensureStorageAccount() error {
	ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
	defer cancel()

	client := getStorageAccountMgmtClient(ts.SubscriptionID, ts.Env)
	accounts, err := client.ListByResourceGroup(ctx, test.ResourceGroupName)
	if err != nil {
		return err
	}

	if accounts.Response.Response == nil {
		return errors.New("response is nil and error is not nil")
	}

	if accounts.Response.Response != nil && accounts.StatusCode == 404 {
		return errors.New("resource group does not exist")
	}

	for _, account := range *accounts.Value {
		if ts.AccountName == *account.Name {
			// provisioned, so return
			return nil
		}
	}

	_, err = client.Create(ctx, test.ResourceGroupName, ts.AccountName, storage.AccountCreateParameters{
		Sku: &storage.Sku{
			Name: storage.StandardLRS,
			Tier: storage.Standard,
		},
		Kind:     storage.BlobStorage,
		Location: common.PtrString(test.Location),
		AccountPropertiesCreateParameters: &storage.AccountPropertiesCreateParameters{
			AccessTier: storage.Hot,
		},
	})

	return err
}

func getStorageAccountMgmtClient(subscriptionID string, env azure.Environment) *storage.AccountsClient {
	client := storage.NewAccountsClientWithBaseURI(env.ResourceManagerEndpoint, subscriptionID)
	a, err := azauth.NewAuthorizerFromEnvironment()
	if err != nil {
		log.Fatal(err)
	}
	client.Authorizer = a
	return &client
}
