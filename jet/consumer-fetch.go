// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"time"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type consumerFetchCaller struct{}

func (caller consumerFetchCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":fetch", len(args), 1, 5)
	consumer := self.Any.(jetstream.Consumer)
	var (
		opts    []jetstream.FetchOpt
		mb      jetstream.MessageBatch
		maxWait time.Duration
		err     error
	)
	batch, ok := args[0].(slip.Fixnum)
	if !ok {
		slip.PanicType(":batch", args[0], "fixnum")
	}
	args = args[1:]
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":max-wait")); has {
		maxWait = mustBeDuration(v, ":max-wait")
		opts = append(opts, jetstream.FetchMaxWait(maxWait))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":heartbeat")); has {
		opts = append(opts, jetstream.FetchHeartbeat(mustBeDuration(v, ":heartbeat")))
	}
	if maxWait <= 0 {
		mb, err = consumer.FetchNoWait(int(batch))
	} else {
		mb, err = consumer.Fetch(int(batch), opts...)
	}
	if err != nil {
		panic(err)
	}
	return MakeMessageBatch(mb)
}

func (caller consumerFetchCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":fetch",
		Text: `Used to retrieve up to a provided number of messages from a
stream. This method will send a single request and deliver either all
requested messages unless time out is met earlier. _:fetch_ timeout
defaults to 30 seconds and can be configured using _:max-wait_
option.


By default, _:fetch_ uses a 5 second idle heartbeat for requests longer than
10 seconds. For shorter requests, the idle heartbeat is disabled.
This can be configured using _:heartbeat_ option. If a client does
not receive a heartbeat message from a stream for more than 2 times
the idle heartbeat setting, _:fetch_ will raise an error.


_:fetch_ is non-blocking and returns a _jet-message-batch_ instance, exposing a channel
for delivered messages.


Messages channel is always closed, thus it is safe to range over it
without additional checks.`,
		Args: []*slip.DocArg{
			{
				Name: "batch",
				Type: "fixnum",
				Text: "The maximum number of bytes in a batch.",
			},
			{Name: "&key"},
			{
				Name: ":max-wait",
				Type: "real",
				Text: "The number of seconds to wait before closing the batch.",
			},
			{
				Name: ":heartbeat",
				Type: "real",
				Text: "The number of seconds in a between heartbeats.",
			},
		},
		Return: "jet-message-batch",
	}
}
