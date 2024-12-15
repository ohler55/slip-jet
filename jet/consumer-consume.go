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
	flavors.CheckMethodArgCount(self, ":consume", len(args), 1, 3)
	consumer := self.Any.(jetstream.Consumer)
	var opts []jetstream.PullConsumeOpt
	msgCaller := cl.ResolveToCaller(s, args[0], 0)

	if v, has := slip.GetArgsKeyValue(args[1:], slip.Symbol(":error-handler")); has {
		errCaller := cl.ResolveToCaller(s, v, 0)
		opts = append(opts, jetstream.ConsumeErrHandler(
			func(cc jetstream.ConsumeContext, err error) {
				_ = errCaller.Call(s, slip.List{MakeConsumeContext(cc), slip.NewError("%s", err)}, 0)
			}))
	}
	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		_ = msgCaller.Call(s, slip.List{MakeMsg(msg)}, 0)
	}, opts...)
	if err != nil {
		panic(err)
	}
	return MakeConsumeContext(cc)
}

func (caller consumerConsumeCaller) Docs() string {
	return `__:consume__ message-handler &key error-handler => _jet-message-context_
   _message-handler_ [function] a function that expects one message argument.
   _:error-handler_ [boolean] a function that is called on error with two arguments. The
first argument is a _jet-consumer-context_ and the second is the error that caused the
handler to be called.


Will continuously receive messages and handle them with the provided callback
function. _:consume_ can be configured using _:error-handler_ options:


Error handling and monitoring can be configured using _:error-handler_ option,
which provides information about errors encountered during consumption
(both transient and terminal)


_:consume_ returns a _jet-consume-context_, which can be used to stop or drain
the consumer.
`
}
