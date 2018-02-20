package eventhub

import (
	"context"
	"runtime"
	"sync"

	"github.com/Azure/azure-event-hubs-go/auth"
	"github.com/Azure/azure-event-hubs-go/cbs"
	"github.com/Azure/go-autorest/autorest/azure"
	log "github.com/sirupsen/logrus"
	"pack.ag/amqp"
)

type (
	namespace struct {
		client        *amqp.Client
		clientMu      sync.Mutex
		name          string
		tokenProvider auth.TokenProvider
		environment   azure.Environment
		Logger        *log.Logger
	}
)

func newNamespace(name string, tokenProvider auth.TokenProvider, env azure.Environment) *namespace {
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

	if ns.client == nil {
		host := ns.getAmqpHostURI()
		client, err := amqp.Dial(
			host,
			amqp.ConnSASLAnonymous(),
			amqp.ConnMaxSessions(65535),
			amqp.ConnProperty("product", "MSGolangClient"),
			amqp.ConnProperty("version", "0.0.1"),
			amqp.ConnProperty("platform", runtime.GOOS),
			amqp.ConnProperty("framework", runtime.Version()),
			amqp.ConnProperty("user-agent", rootUserAgent),
		)
		if err != nil {
			return nil, err
		}
		ns.client = client
	}
	return ns.client, nil
}

func (ns *namespace) negotiateClaim(ctx context.Context, entityPath string) error {
	audience := ns.getEntityAudience(entityPath)
	conn, err := ns.connection()
	if err != nil {
		return err
	}
	return cbs.NegotiateClaim(ctx, audience, conn, ns.tokenProvider)
}

func (ns *namespace) getAmqpHostURI() string {
	return "amqps://" + ns.name + "." + ns.environment.ServiceBusEndpointSuffix + "/"
}

func (ns *namespace) getEntityAudience(entityPath string) string {
	return ns.getAmqpHostURI() + entityPath
}
