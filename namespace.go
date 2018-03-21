package eventhub

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
	"runtime"
	"sync"

	"github.com/Azure/azure-amqp-common-go/auth"
	"github.com/Azure/azure-amqp-common-go/cbs"
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

	if ns.client != nil {
		return ns.client, nil
	}

	host := ns.getAmqpHostURI()
	client, err := amqp.Dial(host,
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
