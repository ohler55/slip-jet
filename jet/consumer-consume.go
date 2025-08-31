// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/cl"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type consumerConsumeCaller struct{}

func (caller consumerConsumeCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":consume", len(args), 1, 17)
	consumer := self.Any.(jetstream.Consumer)

	var opts []jetstream.PullConsumeOpt
	msgCaller := cl.ResolveToCaller(s, args[0], 0)

	args = args[1:]
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":error-handler")); has {
		errCaller := cl.ResolveToCaller(s, v, 0)
		opts = append(opts, jetstream.ConsumeErrHandler(
			func(cc jetstream.ConsumeContext, err error) {
				_ = errCaller.Call(s, slip.List{MakeConsumeContext(cc), slip.ErrorNew(s, depth, "%s", err)}, 0)
			}))
	}
	getPullOptArgs(s, args, func(opt any) { opts = append(opts, opt.(jetstream.PullConsumeOpt)) }, depth)

	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		_ = msgCaller.Call(s, slip.List{MakeMsg(msg)}, 0)
	}, opts...)
	if err != nil {
		panic(err)
	}
	return MakeConsumeContext(cc)
}

func (caller consumerConsumeCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":consume",
		Text: `Will continuously receive messages and handle them with the provided callback
function. _:consume_ can be configured using _:error-handler_ options:


Error handling and monitoring can be configured using _:error-handler_ option,
which provides information about errors encountered during consumption
(both transient and terminal)


Returns a _jet-consume-context_, which can be used to stop or drain the consumer.
`,
		Args: []*slip.DocArg{
			{
				Name: "message-handler",
				Type: "function",
				Text: "A function that expects one message argument.",
			},
			{Name: "&key"},
			{
				Name: ":error-handler",
				Type: "function",
				Text: `A function that is called on error with two arguments. The
first argument is a _jet-consumer-context_ and the second is the error that caused the
handler to be called.`,
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
		Return: "jet-consume-context",
	}
}

func getPullOptArgs(s *slip.Scope, args slip.List, appendOpt func(opt any), depth int) {
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":stop-after")); has {
		appendOpt(jetstream.StopAfter(mustBeInt(s, v, ":stop-after", depth)))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":pull-expiry")); has {
		appendOpt(jetstream.PullExpiry(mustBeDuration(s, v, ":pull-expiry", depth)))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":pull-max-bytes")); has {
		appendOpt(jetstream.PullMaxBytes(mustBeInt(s, v, ":pull-max-bytes", depth)))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":pull-heartbeat")); has {
		appendOpt(jetstream.PullHeartbeat(mustBeDuration(s, v, ":pull-heartbeat", depth)))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":pull-max-messages")); has {
		appendOpt(jetstream.PullMaxMessages(mustBeInt(s, v, ":pull-max-messages", depth)))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":pull-threshold-bytes")); has {
		appendOpt(jetstream.PullThresholdBytes(mustBeInt(s, v, ":pull-threshold-bytes", depth)))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":pull-threshold-messages")); has {
		appendOpt(jetstream.PullThresholdMessages(mustBeInt(s, v, ":pull-threshold-messages", depth)))
	}
}
