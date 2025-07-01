// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/cl"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type consumerConsumeCaller struct{}

func (caller consumerConsumeCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":consume", len(args), 1, 17)
	consumer := self.Any.(jetstream.Consumer)

	var opts []jetstream.PullConsumeOpt
	msgCaller := cl.ResolveToCaller(s, args[0], 0)

	args = args[1:]
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":error-handler")); has {
		errCaller := cl.ResolveToCaller(s, v, 0)
		opts = append(opts, jetstream.ConsumeErrHandler(
			func(cc jetstream.ConsumeContext, err error) {
				_ = errCaller.Call(s, slip.List{MakeConsumeContext(cc), slip.NewError("%s", err)}, 0)
			}))
	}
	getPullOptArgs(args, func(opt any) { opts = append(opts, opt.(jetstream.PullConsumeOpt)) })

	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		_ = msgCaller.Call(s, slip.List{MakeMsg(msg)}, 0)
	}, opts...)
	if err != nil {
		panic(err)
	}
	return MakeConsumeContext(cc)
}

func (caller consumerConsumeCaller) Docs() string {
	return `__:consume__ message-handler &key
error-handler
stop-after
pull-expiry
pull-max-bytes
pull-max-messages
pull-heartbeat
pull-threshold-bytes
pull-threshold-messages
=> _jet-message-context_
   _message-handler_ [function] a function that expects one message argument.
   _:error-handler_ [boolean] a function that is called on error with two arguments. The
first argument is a _jet-consumer-context_ and the second is the error that caused the
handler to be called.
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


Will continuously receive messages and handle them with the provided callback
function. _:consume_ can be configured using _:error-handler_ options:


Error handling and monitoring can be configured using _:error-handler_ option,
which provides information about errors encountered during consumption
(both transient and terminal)


_:consume_ returns a _jet-consume-context_, which can be used to stop or drain
the consumer.
`
}

func getPullOptArgs(args slip.List, appendOpt func(opt any)) {
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":stop-after")); has {
		appendOpt(jetstream.StopAfter(mustBeInt(v, ":stop-after")))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":pull-expiry")); has {
		appendOpt(jetstream.PullExpiry(mustBeDuration(v, ":pull-expiry")))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":pull-max-bytes")); has {
		appendOpt(jetstream.PullMaxBytes(mustBeInt(v, ":pull-max-bytes")))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":pull-heartbeat")); has {
		appendOpt(jetstream.PullHeartbeat(mustBeDuration(v, ":pull-heartbeat")))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":pull-max-messages")); has {
		appendOpt(jetstream.PullMaxMessages(mustBeInt(v, ":pull-max-messages")))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":pull-threshold-bytes")); has {
		appendOpt(jetstream.PullThresholdBytes(mustBeInt(v, ":pull-threshold-bytes")))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":pull-threshold-messages")); has {
		appendOpt(jetstream.PullThresholdMessages(mustBeInt(v, ":pull-threshold-messages")))
	}
}
