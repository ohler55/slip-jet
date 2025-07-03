// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type consumerNextCaller struct{}

func (caller consumerNextCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":next", len(args), 0, 4)
	consumer := self.Any.(jetstream.Consumer)

	var opts []jetstream.FetchOpt
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":max-wait")); has {
		opts = append(opts, jetstream.FetchMaxWait(mustBeDuration(v, ":max-wait")))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":heartbeat")); has {
		opts = append(opts, jetstream.FetchHeartbeat(mustBeDuration(v, ":heartbeat")))
	}
	m, err := consumer.Next(opts...)
	if err != nil {
		panic(err)
	}
	return MakeMsg(m)
}

func (caller consumerNextCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":next",
		Text: `Used to retrieve the next message from the consumer. This
method will block until the message is retrieved or timeout is
reached.`,
		Args: []*slip.DocArg{
			{Name: "&key"},
			{
				Name: ":max-wait",
				Type: "real",
				Text: "The number of seconds to wait before returning.",
			},
			{
				Name: ":heartbeat",
				Type: "real",
				Text: "The number of seconds between heartbeats.",
			},
		},
		Return: "jet-msg",
	}
}
