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
	flavors.CheckMethodArgCount(self, ":next", len(args), 0, 4)
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

func (caller consumerNextCaller) Docs() string {
	return `__:next__ batch &key max-wait heartbeat => _jet-msg_
   _:max-wait_ [real] the number of seconds to wait before returning.
   _:heartbeat_ [real] the number of seconds in a heartbeat.


Used to retrieve the next message from the consumer. This
method will block until the message is retrieved or timeout is
reached.
`
}
