package eventhub

import (
	"fmt"
	"github.com/Azure/azure-event-hubs-go/cbs"
	"github.com/Azure/go-autorest/autorest/azure"
	log "github.com/sirupsen/logrus"
	"pack.ag/amqp"
	"runtime"
	"sync"
)

type (
	namespace struct {
		client        *amqp.Client
		clientMu      sync.Mutex
		name          string
		tokenProvider cbs.TokenProvider
		environment   azure.Environment
		Logger        *log.Logger
		cbsMu         sync.Mutex
		cbsLink       *cbs.Link
	}
)

func newNamespace(name string, tokenProvider cbs.TokenProvider, env azure.Environment) *namespace {
	ns := &namespace{
		name:          name,
		tokenProvider: tokenProvider,
		environment:   env,
		Logger:        log.New(),
	}
	ns.Logger.SetLevel(log.WarnLevel)

	return ns
}

func (ns *namespace) connection() (*amqp.Client, error) {
	ns.clientMu.Lock()
	defer ns.clientMu.Unlock()

	host := ns.getAmqpHostURI()
	client, err := amqp.Dial(
		host,
		amqp.ConnSASLAnonymous(),
		amqp.ConnMaxSessions(65535),
		amqp.ConnProperty("product", "MSGolangClient"),
		amqp.ConnProperty("version", "0.0.1"),
		amqp.ConnProperty("platform", runtime.GOOS),
		amqp.ConnProperty("framework", runtime.Version()),
		amqp.ConnProperty("user-agent", rootUserAgent))
	if err != nil {
		return nil, err
	}
	ns.client = client
	return ns.client, nil
}

func (ns *namespace) getAmqpHostURI() string {
	return fmt.Sprintf("amqps://%s.%s/", ns.name, ns.environment.ServiceBusEndpointSuffix)
}

func (ns *namespace) getEntityAudience(entityPath string) string {
	return ns.getAmqpHostURI() + entityPath
}
