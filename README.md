# slip-jet

A NATS JetStream package for SLIP.

## Summary

This SLIP package makes use of the NATS JetStream Simplified Client
API described at
https://github.com/nats-io/nats.go/blob/main/jetstream/README.md#jetstream-simplified-client
or https://pkg.go.dev/github.com/nats-io/nats.go/jetstream. The
JetStream API is object based and this package uses Flavors to
implement an object based API for SLIP that closely follows the
JetStream API.


--------
Notes

- test use cases (start with connection)
 - with token ??
 - nkeys
 - start server with multiple accounts

- does client instance need to keep track of subscription?
 - so that the error callback with subscription can return the correct instance?

- make mocks for listers and parents to test error methods
 - mockStream
  - ListConsumers(context.Context) jetstream.ConsumerInfoLister {
  - ConsumerNames(context.Context) jetstream.ConsumerNameLister {
 - mockClient
  - ListStreams
  - StreamNames

- consumer
 - stream
  + create-consumer
  + update-consumer
  + create-or-update-consumer
  + ordered-consumer
  - get-consumer (name &key timeout) => consumer
   - return nil if not found instead of panic
  - delete-consumer (name &key timeout)
  - list-consumers (&key timeout)
   - build list and return
  - consumer-names (&key timeout)
   - build list and return

 - client
  - create-consumer (stream &key timeout ...) => consumer
   - all consumer config options
  - update-consumer (stream &key timeout ...) => consumer
  - create-or-update-consumer (stream &key timeout ...) => consumer
  - ordered-consumer (stream &key timeout ...) => consumer
   - ordered consumer config options
  - get-consumer (stream consumer &key timeout) => consumer
  - delete-consumer (stream consumer &key timeout)
 - jet-messages-context
  - test with mock


- should managers be included in the objects so the api is more friendly?
 - need a struct for each and not just the current assignment to Any
  - methods
   - :update
   - :client
   - :delete

 - stream
  - consumer manager functions (create, update, get, delete, list, names, ordered, create-or-update)

 - as StreamConsumerManager
 - as AccountInfo
  - auth
   - support and test auth
    - users, accounts, nkeys, username, password



- don't implement both client and stream APIs for pub sub, just stream
- add publisher with saved PublishOpts
 - also tied to stream (but not subject?)
 - tied to sync vs async
 - publish just data abd form msg from that


- flavors
 - jetstream
 - stream
 - consumer
  - create from jetstream object or consumer :init
   - jetstream follows api
   - :init keep jetstream more "trim"
 - msg
  - need to be able to create a message
   - publish from jetstream
  - test after jetstream can be created
   - consumer needed as well

- general, factory or make
 - factory matches jetstream api
 - make is maybe more lispy
 - pick and approach and use it throughout
 - if adding a new client
  - factory requires support from the top and touches all
  - make can make use of an alternate top or alternatives for testing
