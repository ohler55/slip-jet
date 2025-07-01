// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type clientOrderedConsumerCaller struct{}

func (caller clientOrderedConsumerCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":ordered-consumer", len(args), 1, len(orderedOptMap)*2+3)
	js := self.Any.(*Client).js

	stream := slip.MustBeString(args[0], "stream")

	args = args[1:]
	ctx := context.Background()

	var cfg jetstream.OrderedConsumerConfig
	InitOrderedConfig(&cfg, args)

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	consumer, err := js.OrderedConsumer(ctx, stream, cfg)
	if err != nil {
		panic(err)
	}
	return MakeConsumer(consumer)
}

func (caller clientOrderedConsumerCaller) Docs() string {
	return makeConsumerMethodDoc(":ordered-consumer", "_stream_ ", "<jet-consumer>",
		"   _stream_ [string] the name of the stream.",
		`Returns an OrderedConsumer as a _jet-consumer_ instance. OrderedConsumers
are managed by the library and provide a simple way to consume
messages from a stream. Ordered consumers are ephemeral in-memory
pull consumers and are resilient to deletes and restarts.
`)
}
