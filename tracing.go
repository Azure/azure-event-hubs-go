package eventhub

import (
	"context"
	"net/http"
	"os"

	"github.com/opentracing/opentracing-go"
	tag "github.com/opentracing/opentracing-go/ext"
)

func (h *Hub) startSpanFromContext(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
	ApplyComponentInfo(span)
	return span, ctx
}

func (ns *namespace) startSpanFromContext(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
	ApplyComponentInfo(span)
	return span, ctx
}

func (s *sender) startProducerSpanFromContext(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
	ApplyComponentInfo(span)
	tag.SpanKindProducer.Set(span)
	tag.MessageBusDestination.Set(span, s.getFullIdentifier())
	return span, ctx
}

func (r *receiver) startConsumerSpanFromContext(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
	ApplyComponentInfo(span)
	tag.SpanKindConsumer.Set(span)
	tag.MessageBusDestination.Set(span, r.getFullIdentifier())
	return span, ctx
}

func (r *receiver) startConsumerSpanFromWire(ctx context.Context, operationName string, reference opentracing.SpanContext, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	opts = append(opts, opentracing.FollowsFrom(reference))
	span := opentracing.StartSpan(operationName, opts...)
	ctx = opentracing.ContextWithSpan(ctx, span)
	ApplyComponentInfo(span)
	tag.SpanKindConsumer.Set(span)
	tag.MessageBusDestination.Set(span, r.getFullIdentifier())
	return span, ctx
}

func (em *entityManager) startSpanFromContext(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
	ApplyComponentInfo(span)
	tag.SpanKindRPCClient.Set(span)
	return span, ctx
}

// ApplyComponentInfo applies eventhub library and network info to the span
func ApplyComponentInfo(span opentracing.Span) {
	tag.Component.Set(span, "github.com/Azure/azure-event-hubs-go")
	span.SetTag("version", Version)
	applyNetworkInfo(span)
}

func applyNetworkInfo(span opentracing.Span) {
	hostname, err := os.Hostname()
	if err == nil {
		tag.PeerHostname.Set(span, hostname)
	}
}

func applyRequestInfo(span opentracing.Span, req *http.Request) {
	tag.HTTPUrl.Set(span, req.URL.String())
	tag.HTTPMethod.Set(span, req.Method)
}

func applyResponseInfo(span opentracing.Span, res *http.Response) {
	tag.HTTPStatusCode.Set(span, uint16(res.StatusCode))
}
