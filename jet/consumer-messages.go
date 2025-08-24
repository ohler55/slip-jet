// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type consumerMessagesCaller struct{}

func (caller consumerMessagesCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":messages", len(args), 0, 16)
	consumer := self.Any.(jetstream.Consumer)
	var opts []jetstream.PullMessagesOpt

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":error-on-missing-heartbeat")); has {
		opts = append(opts, jetstream.WithMessagesErrOnMissingHeartbeat(v != nil))
	}
	getPullOptArgs(s, args, func(opt any) { opts = append(opts, opt.(jetstream.PullMessagesOpt)) }, depth)

	mc, err := consumer.Messages(opts...)
	if err != nil {
		panic(err)
	}
	return MakeMessagesContext(mc)
}

func (caller consumerMessagesCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":messages",
		Text: `Returns _jet-messages-context_, allowing continuously iterating
over messages on a stream.


Messages can be optimized for throughput or memory usage using
_:pull-expiry_, _:pull-max-messages_, _:pull-max-bytes_ and _:pull-heartbeat_
consumer configuration options. Unless there is a specific use case, these
options should not be used.


_:error-on-missing-heartbeat_ can be used to enable/disable
erroring out on the _jet-messages-context_ _:next_ method when a
heartbeat is missing. This option is enabled by default.`,
		Args: []*slip.DocArg{
			{Name: "&key"},
			{
				Name: ":error-on-missing-heartbeat",
				Type: "boolean",
				Text: `Sets whether a missing heartbeat error should be
raised when calling [jet-messages-context :next].`,
				Default: slip.True,
			},
			{
				Name: ":stop-after",
				Type: "fixnum",
				Text: `Sets the number of messages after which the consumer is
automatically stopped and no more messages are pulled from the server.`,
			},
			{
				Name: ":pull-expiry",
				Type: "real",
				Text: `Sets timeout in second on a single pull request, waiting until
at least one message is available.`,
				Default: slip.Fixnum(30),
			},
			{
				Name: ":pull-max-bytes",
				Type: "fixnum",
				Text: `Limits the number of bytes to be buffered in the client.
If not provided, the limit is not set (max messages will be used instead). This option
is exclusive with _:pull-max-messages_.`,
			},
			{
				Name: ":pull-max-messages",
				Type: "fixnum",
				Text: `Limits the number of messages to be buffered in the
client. This option is exclusive with _:pull-max-bytes_.`,
				Default: slip.Fixnum(500),
			},
			{
				Name: ":pull-heartbeat",
				Type: "real",
				Text: `Sets the idle heartbeat duration for a pull subscription.
If a client does not receive a heartbeat message from a stream for more than the idle
heartbeat setting, the subscription will be removed and error will be passed to the
message handler. If not provided, a default _:pull-expiry_ / 2 will be used (capped at 30 seconds).`,
			},
			{
				Name: ":pull-threshold-bytes",
				Type: "fixnum",
				Text: `Sets the byte count on which Consume will trigger
new pull request to the server. Defaults to 50% of _:max-bytes_ (if set).`,
			},
			{
				Name: ":pull-threshold-messages",
				Type: "fixnum",
				Text: `Sets the message count on which Consume will
trigger new pull request to the server. Defaults to 50% of _:max-messages_.`,
			},
		},
		Return: "jet-message-context",
	}
}
