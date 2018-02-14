package eventhub

import (
	"context"
	"flag"
	"fmt"
	mgmt "github.com/Azure/azure-sdk-for-go/services/eventhub/mgmt/2017-04-01/eventhub"
	rm "github.com/Azure/azure-sdk-for-go/services/resources/mgmt/2017-05-10/resources"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"math/rand"
	"os"
	"pack.ag/amqp"
	"sync"
	"testing"
	"time"
)

var (
	letterRunes = []rune("abcdefghijklmnopqrstuvwxyz123456789")
	debug       = flag.Bool("debug", false, "output debug level logging")
)

const (
	Location          = "eastus"
	ResourceGroupName = "ehtest"
)

type (
	// eventHubSuite encapsulates a end to end test of Event Hubs with build up and tear down of all EH resources
	eventHubSuite struct {
		suite.Suite
		tenantID       string
		subscriptionID string
		clientID       string
		clientSecret   string
		namespace      string
		env            azure.Environment
		armToken       *adal.ServicePrincipalToken
	}
)

func init() {
	rand.Seed(time.Now().Unix())
}

func TestServiceBusSuite(t *testing.T) {
	suite.Run(t, new(eventHubSuite))
}

func (suite *eventHubSuite) SetupSuite() {
	flag.Parse()
	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	suite.tenantID = mustGetEnv("AZURE_TENANT_ID")
	suite.subscriptionID = mustGetEnv("AZURE_SUBSCRIPTION_ID")
	suite.clientID = mustGetEnv("AZURE_CLIENT_ID")
	suite.clientSecret = mustGetEnv("AZURE_CLIENT_SECRET")
	suite.namespace = mustGetEnv("EVENTHUB_NAMESPACE")
	suite.env = azure.PublicCloud
	suite.armToken = suite.servicePrincipalToken()

	err := suite.ensureProvisioned(mgmt.SkuTierStandard)
	if err != nil {
		log.Fatalln(err)
	}
}

func (suite *eventHubSuite) TearDownSuite() {
	// tear down queues and subscriptions maybe??
}

func (suite *eventHubSuite) TestBasicOperations() {
	tests := map[string]func(*testing.T, *Namespace, *mgmt.Model){
		"TestSend":           testBasicSend,
		"TestSendAndReceive": testBasicSendAndReceive,
	}

	ns := suite.getNamespace()

	for name, testFunc := range tests {
		setupTestTeardown := func(t *testing.T) {
			hubName := randomName("goehtest", 10)
			mgmtHub, err := ns.EnsureEventHub(context.Background(), hubName)
			defer ns.DeleteEventHub(context.Background(), hubName)

			if err != nil {
				t.Fatal(err)
			}

			testFunc(t, ns, mgmtHub)
		}

		suite.T().Run(name, setupTestTeardown)
	}
}

func testBasicSend(t *testing.T, ns *Namespace, mgmtHub *mgmt.Model) {
	hub, err := ns.NewEventHub(*mgmtHub.Name)
	if err != nil {
		t.Fatal(err)
	}

	err = hub.Send(context.Background(), &amqp.Message{
		Data: []byte("Hello!"),
	})
	assert.Nil(t, err)
}

func testBasicSendAndReceive(t *testing.T, ns *Namespace, mgmtHub *mgmt.Model) {
	partitionID := (*mgmtHub.PartitionIds)[0]
	hub, err := ns.NewEventHub(*mgmtHub.Name, HubWithPartitionedSender(partitionID))
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()

	numMessages := rand.Intn(100) + 20
	var wg sync.WaitGroup
	wg.Add(numMessages + 1)

	messages := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		messages[i] = randomName("hello", 10)
	}

	go func() {
		for idx, message := range messages {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := hub.Send(ctx, &amqp.Message{Data: []byte(message)}, SendWithMessageID(fmt.Sprintf("%d", idx)))
			cancel()
			if err != nil {
				log.Println(idx)
				log.Fatalln(err)
			}
		}
		defer wg.Done()
	}()

	count := 0
	err = hub.Receive(partitionID, func(ctx context.Context, msg *amqp.Message) error {
		assert.Equal(t, messages[count], string(msg.Data))
		count++
		wg.Done()
		return nil
	}, ReceiveWithPrefetchCount(100))
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

}

func (suite *eventHubSuite) ensureProvisioned(tier mgmt.SkuTier) error {
	_, err := EnsureResourceGroup(context.Background(), suite.subscriptionID, ResourceGroupName, Location, suite.armToken, suite.env)
	if err != nil {
		return err
	}

	_, err = suite.ensureNamespace()
	if err != nil {
		return err
	}

	return nil
}

func (suite *eventHubSuite) servicePrincipalToken() *adal.ServicePrincipalToken {

	oauthConfig, err := adal.NewOAuthConfig(suite.env.ActiveDirectoryEndpoint, suite.tenantID)
	if err != nil {
		log.Fatalln(err)
	}

	tokenProvider, err := adal.NewServicePrincipalToken(*oauthConfig,
		suite.clientID,
		suite.clientSecret,
		suite.env.ResourceManagerEndpoint)
	if err != nil {
		log.Fatalln(err)
	}

	return tokenProvider
}

func (suite *eventHubSuite) ensureResourceGroup() (*rm.Group, error) {
	group, err := EnsureResourceGroup(context.Background(), suite.subscriptionID, suite.namespace, Location, suite.armToken, suite.env)
	if err != nil {
		return nil, err
	}
	return group, err
}

func (suite *eventHubSuite) ensureNamespace() (*mgmt.EHNamespace, error) {
	ns, err := EnsureNamespace(context.Background(), suite.subscriptionID, ResourceGroupName, suite.namespace, Location, suite.armToken, suite.env)
	if err != nil {
		return nil, err
	}
	return ns, err
}

func (suite *eventHubSuite) getNamespace() *Namespace {
	return getNamespace(suite.tenantID, suite.subscriptionID, suite.namespace, suite.clientID, suite.clientSecret, suite.env)
}

func getNamespace(tenantID, subscriptionID, namespace, appID, secret string, env azure.Environment) *Namespace {
	cred := ServicePrincipalCredentials{
		TenantID:      tenantID,
		ApplicationID: appID,
		Secret:        secret,
	}

	ns, err := NewNamespaceWithServicePrincipalCredentials(subscriptionID, ResourceGroupName, namespace, cred, env)
	if err != nil {
		log.Fatalln(err)
	}
	return ns
}

func mustGetEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic("env variable '" + key + "' required for integration tests.")
	}
	return v
}

func randomName(prefix string, length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return prefix + "-" + string(b)
}
