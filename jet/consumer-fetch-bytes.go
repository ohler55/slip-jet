// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"time"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type consumerFetchBytesCaller struct{}

func (caller consumerFetchBytesCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":fetch", len(args), 1, 5)
	consumer := self.Any.(jetstream.Consumer)
	var (
		opts    []jetstream.FetchOpt
		mb      jetstream.MessageBatch
		maxWait time.Duration
		err     error
	)
	maxBytes, ok := args[0].(slip.Fixnum)
	if !ok {
		slip.PanicType(":max-bytes", args[0], "fixnum")
	}
	args = args[1:]
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":max-wait")); has {
		maxWait = mustBeDuration(v, ":max-wait")
		opts = append(opts, jetstream.FetchMaxWait(maxWait))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":heartbeat")); has {
		opts = append(opts, jetstream.FetchHeartbeat(mustBeDuration(v, ":heartbeat")))
	}
	mb, err = consumer.FetchBytes(int(maxBytes), opts...)
	if err != nil {
		panic(err)
	}
	return MakeMessageBatch(mb)
}

func (caller consumerFetchBytesCaller) Docs() string {
	return `__:fetch-bytes__ batch &key max-wait heartbeat => _jet-message-batch_
   _max-bytes_ [fixnum] the maximum number of bytes in a batch
   _:max-wait_ [real] the number of seconds to wait before closing the batch.
   _:heartbeat_ [real] the number of seconds in a heartbeat.


Used to retrieve up to a provided bytes from the
stream. This method will send a single request and deliver the
provided number of bytes unless time out is met earlier. _:fetch-bytes_
timeout defaults to 30 seconds and can be configured using
_:max-wait_ option.


By default, _:fetch-bytes_ uses a 5 second idle heartbeat for requests longer than
10 seconds. For shorter requests, the idle heartbeat is disabled.
This can be configured using _:heartbeat_ option. If a client does
not receive a heartbeat message from a stream for more than 2 times
the idle heartbeat setting, _:fetch-bytes_ will raise and error.


_:fetch-bytes_ is non-blocking and returns _jet-message-batch_, exposing a channel
for delivered messages.


Messages channel is always closed, thus it is safe to range over it
without additional checks.
`
}
