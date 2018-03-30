package test

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
	"flag"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-amqp-common-go"
	mgmt "github.com/Azure/azure-sdk-for-go/services/eventhub/mgmt/2017-04-01/eventhub"
	rm "github.com/Azure/azure-sdk-for-go/services/resources/mgmt/2017-05-10/resources"
	"github.com/Azure/go-autorest/autorest/azure"
	azauth "github.com/Azure/go-autorest/autorest/azure/auth"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

var (
	letterRunes = []rune("abcdefghijklmnopqrstuvwxyz123456789")
	debug       = flag.Bool("debug", false, "output debug level logging")
)

const (
	// Location is the Azure geographic location the test suite will use for provisioning
	Location = "eastus"

	// ResourceGroupName is the name of the resource group the test suite will use for provisioning
	ResourceGroupName = "ehtest"
)

type (
	// BaseSuite encapsulates a end to end test of Event Hubs with build up and tear down of all EH resources
	BaseSuite struct {
		suite.Suite
		SubscriptionID string
		Namespace      string
		Env            azure.Environment
		TagID          string
	}

	// HubMgmtOption represents an option for configuring an Event Hub.
	HubMgmtOption func(model *mgmt.Model) error
	// NamespaceMgmtOption represents an option for configuring a Namespace
	NamespaceMgmtOption func(ns *mgmt.EHNamespace) error
)

func init() {
	rand.Seed(time.Now().Unix())
}

// HubWithPartitions configures an Event Hub to have a specific number of partitions.
//
// Must be between 1 and 32
func HubWithPartitions(count int) HubMgmtOption {
	return func(model *mgmt.Model) error {
		if count < 1 || count > 32 {
			return errors.New("count must be between 1 and 32")
		}
		model.PartitionCount = common.PtrInt64(int64(count))
		return nil
	}
}

// SetupSuite constructs the test suite from the environment and
func (suite *BaseSuite) SetupSuite() {
	flag.Parse()
	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	suite.SubscriptionID = mustGetEnv("AZURE_SUBSCRIPTION_ID")
	suite.Namespace = mustGetEnv("EVENTHUB_NAMESPACE")
	envName := os.Getenv("AZURE_ENVIRONMENT")
	suite.TagID = RandomString("tag", 10)

	if envName == "" {
		suite.Env = azure.PublicCloud
	} else {
		var err error
		env, err := azure.EnvironmentFromName(envName)
		if err != nil {
			log.Fatal(err)
		}
		suite.Env = env
	}

	err := suite.ensureProvisioned(mgmt.SkuTierStandard)
	if err != nil {
		log.Fatalln(err)
	}
}

// TearDownSuite might one day destroy all of the resources in the suite, but I'm not sure we want to do that just yet...
func (suite *BaseSuite) TearDownSuite() {
	// maybe tear down all existing resource??
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	suite.deleteAllTaggedEventHubs(ctx)
}

// EnsureEventHub creates an Event Hub if it doesn't exist
func (suite *BaseSuite) EnsureEventHub(ctx context.Context, name string, opts ...HubMgmtOption) (*mgmt.Model, error) {
	client := suite.getEventHubMgmtClient()
	hub, err := client.Get(ctx, ResourceGroupName, suite.Namespace, name)

	if err != nil {
		newHub := &mgmt.Model{
			Name: &name,
			Properties: &mgmt.Properties{
				PartitionCount: common.PtrInt64(4),
			},
		}

		for _, opt := range opts {
			err = opt(newHub)
			if err != nil {
				return nil, err
			}
		}

		hub, err = client.CreateOrUpdate(ctx, ResourceGroupName, suite.Namespace, name, *newHub)
		if err != nil {
			return nil, err
		}
	}
	return &hub, nil
}

// DeleteEventHub deletes an Event Hub within the given Namespace
func (suite *BaseSuite) DeleteEventHub(ctx context.Context, name string) error {
	client := suite.getEventHubMgmtClient()
	_, err := client.Delete(ctx, ResourceGroupName, suite.Namespace, name)
	return err
}

func (suite *BaseSuite) deleteAllTaggedEventHubs(ctx context.Context) {
	client := suite.getEventHubMgmtClient()
	res, err := client.ListByNamespace(ctx, ResourceGroupName, suite.Namespace)
	if err != nil {
		suite.FailNow(err.Error())
	}

	for res.NotDone() {
		for _, val := range res.Values() {
			if strings.Contains(suite.TagID, *val.Name) {
				client.Delete(ctx, ResourceGroupName, suite.Namespace, *val.Name)
			}
		}
		res.Next()
	}
}

func (suite *BaseSuite) ensureProvisioned(tier mgmt.SkuTier) error {
	_, err := ensureResourceGroup(context.Background(), suite.SubscriptionID, ResourceGroupName, Location, suite.Env)
	if err != nil {
		return err
	}

	_, err = suite.ensureNamespace()
	return err
}

// ensureResourceGroup creates a Azure Resource Group if it does not already exist
func ensureResourceGroup(ctx context.Context, subscriptionID, name, location string, env azure.Environment) (*rm.Group, error) {
	groupClient := getRmGroupClientWithToken(subscriptionID, env)
	group, err := groupClient.Get(ctx, name)

	if group.StatusCode == http.StatusNotFound {
		group, err = groupClient.CreateOrUpdate(ctx, name, rm.Group{Location: common.PtrString(location)})
		if err != nil {
			return nil, err
		}
	} else if group.StatusCode >= 400 {
		return nil, err
	}

	return &group, nil
}

// ensureNamespace creates a Azure Event Hub Namespace if it does not already exist
func ensureNamespace(ctx context.Context, subscriptionID, rg, name, location string, env azure.Environment, opts ...NamespaceMgmtOption) (*mgmt.EHNamespace, error) {
	_, err := ensureResourceGroup(ctx, subscriptionID, rg, location, env)
	if err != nil {
		return nil, err
	}

	client := getNamespaceMgmtClientWithToken(subscriptionID, env)
	namespace, err := client.Get(ctx, rg, name)
	if err != nil {
		return nil, err
	}

	if namespace.StatusCode == 404 {
		newNamespace := &mgmt.EHNamespace{
			Name: &name,

			Sku: &mgmt.Sku{
				Name:     mgmt.Basic,
				Tier:     mgmt.SkuTierBasic,
				Capacity: common.PtrInt32(1),
			},
			EHNamespaceProperties: &mgmt.EHNamespaceProperties{
				IsAutoInflateEnabled:   common.PtrBool(false),
				MaximumThroughputUnits: common.PtrInt32(1),
			},
		}

		for _, opt := range opts {
			err = opt(newNamespace)
			if err != nil {
				return nil, err
			}
		}

		nsFuture, err := client.CreateOrUpdate(ctx, rg, name, *newNamespace)
		if err != nil {
			return nil, err
		}

		namespace, err = nsFuture.Result(*client)
		if err != nil {
			return nil, err
		}
	} else if namespace.StatusCode >= 400 {
		return nil, err
	}

	return &namespace, nil
}

func (suite *BaseSuite) getEventHubMgmtClient() *mgmt.EventHubsClient {
	client := mgmt.NewEventHubsClientWithBaseURI(suite.Env.ResourceManagerEndpoint, suite.SubscriptionID)
	a, err := azauth.NewAuthorizerFromEnvironment()
	if err != nil {
		log.Fatal(err)
	}
	client.Authorizer = a
	return &client
}

func (suite *BaseSuite) ensureNamespace() (*mgmt.EHNamespace, error) {
	ns, err := ensureNamespace(context.Background(), suite.SubscriptionID, ResourceGroupName, suite.Namespace, Location, suite.Env)
	if err != nil {
		return nil, err
	}
	return ns, err
}

func getNamespaceMgmtClientWithToken(subscriptionID string, env azure.Environment) *mgmt.NamespacesClient {
	client := mgmt.NewNamespacesClientWithBaseURI(env.ResourceManagerEndpoint, subscriptionID)
	a, err := azauth.NewAuthorizerFromEnvironment()
	if err != nil {
		log.Fatal(err)
	}
	client.Authorizer = a
	return &client
}

func getRmGroupClientWithToken(subscriptionID string, env azure.Environment) *rm.GroupsClient {
	groupsClient := rm.NewGroupsClientWithBaseURI(env.ResourceManagerEndpoint, subscriptionID)
	a, err := azauth.NewAuthorizerFromEnvironment()
	if err != nil {
		log.Fatal(err)
	}
	groupsClient.Authorizer = a
	return &groupsClient
}

func mustGetEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic("Env variable '" + key + "' required for integration tests.")
	}
	return v
}

// RandomName generates a random Event Hub name tagged with the suite id
func (suite *BaseSuite) RandomName(prefix string, length int) string {
	return RandomString(prefix, length) + "-" + suite.TagID
}

// RandomString generates a random string with prefix
func RandomString(prefix string, length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return prefix + "-" + string(b)
}
