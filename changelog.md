# Change Log

## 'v0.2.1'
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
