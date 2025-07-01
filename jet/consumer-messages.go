// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type consumerMessagesCaller struct{}

func (caller consumerMessagesCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":messages", len(args), 0, 16)
	consumer := self.Any.(jetstream.Consumer)
	var opts []jetstream.PullMessagesOpt

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":error-on-missing-heartbeat")); has {
		opts = append(opts, jetstream.WithMessagesErrOnMissingHeartbeat(v != nil))
	}
	getPullOptArgs(args, func(opt any) { opts = append(opts, opt.(jetstream.PullMessagesOpt)) })

	mc, err := consumer.Messages(opts...)
	if err != nil {
		panic(err)
	}
	return MakeMessagesContext(mc)
}

func (caller consumerMessagesCaller) Docs() string {
	return `__:messages__ &key
error-on-missing-heartbeat
stop-after
pull-expiry
pull-max-bytes
pull-max-messages
pull-heartbeat
pull-threshold-bytes
pull-threshold-messages
=> _jet-message-context_
   _:error-on-missing-heartbeat_ [boolean] sets whether a missing heartbeat error should be
raised when calling [jet-messages-context :next] (Default: true).
   _:stop-after_ [fixnum] sets the number of messages after which the consumer is
automatically stopped and no more messages are pulled from the server.
   _:pull-expiry_ [real] sets timeout insecond on a single pull request, waiting until
at least one message is available. If not provided, a default of 30 seconds will be used.
   _:pull-max-bytes_ [fixnum] limits the number of bytes to be buffered in the client.
If not provided, the limit is not set (max messages will be used instead). This option
is exclusive with _:pull-max-messages_.
   _:pull-max-messages_ [fixnum] limits the number of messages to be buffered in the
client. If not provided, a default of 500 messages will be used. This option is
exclusive with _:pull-max-bytes_.
   _:pull-heartbeat_ [real] sets the idle heartbeat duration for a pull subscription.
If a client does not receive a heartbeat message from a stream for more than the idle
heartbeat setting, the subscription will be removed and error will be passed to the
message handler. If not provided, a default _:pull-expiry_ / 2 will be used (capped at 30 seconds).
   _:pull-threshold-bytes_ [fixnum] sets the byte count on which Consume will trigger
new pull request to the server. Defaults to 50% of _:max-bytes_ (if set).
   _:pull-threshold-messages_ [fixnum] sets the message count on which Consume will
trigger new pull request to the server. Defaults to 50% of _:max-messages_.


Returns _jet-messages-context_, allowing continuously iterating
over messages on a stream.


Messages can be optimized for throughput or memory usage using
_:pull-expiry_, _:pull-max-messages_, _:pull-max-bytes_ and _:pull-heartbeat_
consumer configuration options. Unless there is a specific use case, these
options should not be used.


_:error-on-missing-heartbeat_ can be used to enable/disable
erroring out on the _jet-messages-context_ _:next_ method when a
heartbeat is missing. This option is enabled by default.
`
}
