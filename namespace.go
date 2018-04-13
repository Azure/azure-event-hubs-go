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

	"github.com/Azure/azure-amqp-common-go/auth"
	"github.com/Azure/azure-amqp-common-go/cbs"
	"github.com/Azure/go-autorest/autorest/azure"
	"pack.ag/amqp"
)

type (
	namespace struct {
		name          string
		tokenProvider auth.TokenProvider
		environment   azure.Environment
	}
)

func newNamespace(name string, tokenProvider auth.TokenProvider, env azure.Environment) *namespace {
	ns := &namespace{
		name:          name,
		tokenProvider: tokenProvider,
		environment:   env,
	}

	return ns
}

func (ns *namespace) newConnection() (*amqp.Client, error) {
	host := ns.getAmqpHostURI()
	return amqp.Dial(host,
		amqp.ConnSASLAnonymous(),
		amqp.ConnMaxSessions(65535),
		amqp.ConnProperty("product", "MSGolangClient"),
		amqp.ConnProperty("version", Version),
		amqp.ConnProperty("platform", runtime.GOOS),
		amqp.ConnProperty("framework", runtime.Version()),
		amqp.ConnProperty("user-agent", rootUserAgent),
	)
}

func (ns *namespace) negotiateClaim(ctx context.Context, conn *amqp.Client, entityPath string) error {
	span, ctx := ns.startSpanFromContext(ctx, "eventhub.namespace.negotiateClaim")
	defer span.Finish()

	audience := ns.getEntityAudience(entityPath)
	return cbs.NegotiateClaim(ctx, audience, conn, ns.tokenProvider)
}

func (ns *namespace) getAmqpHostURI() string {
	return "amqps://" + ns.name + "." + ns.environment.ServiceBusEndpointSuffix + "/"
}

func (ns *namespace) getEntityAudience(entityPath string) string {
	return ns.getAmqpHostURI() + entityPath
}
