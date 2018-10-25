# Change Log

## `head`

## `v1.0.1`
- fix the breaking change from storage; this is not a breaking change for this library
- move from dep to go modules

## `v1.0.0`
- change from OpenTracing to OpenCensus
- add more documentation for EPH
- variadic mgmt options

## `v0.4.0`
- add partition key to received event [#43](https://github.com/Azure/azure-event-hubs-go/pull/43)
- remove `Receive` in eph in favor of `RegisterHandler`, `UnregisterHandler` and `RegisteredHandlerIDs` [#45](https://github.com/Azure/azure-event-hubs-go/pull/45)

## `v0.3.1`
- simplify environmental construction by prefering SAS

## `v0.3.0`
- pin version of amqp

## `v0.2.1`
- update dependency on common to 0.3.2 to fix retry returning nil error

## `v0.2.0`
- add opentracing support
- add context to close functions (breaking change)

## `v0.1.2`
- remove an extraneous dependency on satori/uuid

## `v0.1.1`
- update common dependency to 0.2.4
- provide more feedback when sending using testhub
- retry send upon server-busy
- use a new connection for each sender and receiver

## `v0.1.0`
- initial release
- basic send and receive
- batched send
- offset persistence
- alpha event host processor with Azure storage persistence
- enabled prefetch batching
