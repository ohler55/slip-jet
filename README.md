# slip-jet

A NATS JetStream package for SLIP.

## Summary

This SLIP package makes use of the NATS JetStream Simplified Client
API described at
https://github.com/nats-io/nats.go/blob/main/jetstream/README.md#jetstream-simplified-client
or https://pkg.go.dev/github.com/nats-io/nats.go/jetstream. The
JetStream API is object based and this package uses Flavors to
implement an object based API for SLIP.


--------
Notes

- test use cases (start with connection)
 - with token ??
 - nkeys
 - start server with multiple accounts

- does client instance need to keep track of subscription?
 - so that the error callback with subscription can return the correct instance?

- client
 - as StreamManager
  - :create-stream (name &key ...) [also make-instance with a client and lots of &key options]
  - :update-stream [just :update on stream]
  - :create-or-update-stream
  - :get-stream or maybe just :stream on a client
  - :stream-name-by-subject
  - :delete-stream [or use stream to :delete]
  - :list-streams
  - :stream-names [use for loop on channel then check for error]
 - stream flavor [Any = jetstream.Stream]
  - :update
  - :delete
  - :name
  - :subjects
  - :info (&key cached deleted filter) [property or assoc for all StreamConfig options]
  - :purge (&key keep sequence-number subject)
  - :get-msg (seq &key subject deleted)
  - :delete-msg (seq &key secure)
  - :get-last-msg-for-subject
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
