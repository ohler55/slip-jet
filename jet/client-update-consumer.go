// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type clientUpdateConsumerCaller struct{}

func (caller clientUpdateConsumerCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":update-consumer", len(args), 1, len(consumerOptMap)*2+3)
	js := self.Any.(*Client).js

	stream := slip.MustBeString(args[0], "stream")

	args = args[1:]
	ctx := context.Background()

	var cfg jetstream.ConsumerConfig
	InitConsumerConfig(&cfg, args)

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	consumer, err := js.UpdateConsumer(ctx, stream, cfg)
	if err != nil {
		panic(err)
	}
	return MakeConsumer(consumer)
}

func (caller clientUpdateConsumerCaller) Docs() string {
	return makeConsumerMethodDoc(":update-consumer", "_stream_ ", "<jet-consumer>",
		"   _stream_ [string] the name of the stream.",
		`Updates an existing consumer. If consumer does not
exist an error is raised otherwise a _jet-consumer_ is
returned, allowing operations on a consumer (e.g. fetch messages).
`)
}
