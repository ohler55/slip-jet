// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type streamOrderedConsumerCaller struct{}

func (caller streamOrderedConsumerCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":ordered-consumer", len(args), 0, len(orderedOptMap)*2+2)
	stream := self.Any.(jetstream.Stream)

	var cfg jetstream.OrderedConsumerConfig
	ctx := context.Background()
	InitOrderedConfig(&cfg, args)

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	consumer, err := stream.OrderedConsumer(ctx, cfg)
	if err != nil {
		panic(err)
	}
	return MakeConsumer(consumer)
}

func (caller streamOrderedConsumerCaller) Docs() string {
	return makeOrderedMethodDoc(":ordered-consumer", "", "<jet-consumer>", "",
		`Returns a _jet-consumer_ instance. Ordered consumers are managed by the
library and provide a simple way to consume messages from a stream. Ordered
consumers are ephemeral in-memory pull consumers and are resilient to deletes and restarts.
`)
}
