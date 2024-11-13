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

- client :options
 - compare nats options to proplist
  - booleans are t and nil
  - functions are either nil or the set original value

- create client
 - add all options
 - get for nats option, use for testing as well
  - return assoc with init keywords as keys

 - StreamConsumerManager
 - StreamManager
 - AccountInfo
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
