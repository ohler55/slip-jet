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

- in slip pkg/flavors/Instance.Equal
 - if Any is set then use alt.Diff to check for equal

- in get-consumer test, use equal to compare instances

- consumer
 - stream
  + create-consumer
  + update-consumer
  + create-or-update-consumer
  + ordered-consumer
  + get-consumer
  + list-consumers
  + consumer-names
  + delete-consumer

 - client
  - create-consumer (stream &key timeout ...) => consumer
   - all consumer config options
  - update-consumer (stream &key timeout ...) => consumer
  - create-or-update-consumer (stream &key timeout ...) => consumer
  - ordered-consumer (stream &key timeout ...) => consumer
   - ordered consumer config options
  - get-consumer (stream consumer &key timeout) => consumer
  - delete-consumer (stream consumer &key timeout)


- should managers be included in the objects so the api is more friendly?
 - need a struct for each and not just the current assignment to Any
  - methods
   - :update
   - :client
   - :delete

 - as AccountInfo
  - auth
   - support and test auth
    - users, accounts, nkeys, username, password


- add publisher with saved PublishOpts
 - also tied to stream (but not subject?)
 - tied to sync vs async
 - publish just data and form msg from that
