// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type streamUpdateConsumerCaller struct{}

func (caller streamUpdateConsumerCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":update-consumer", len(args), 0, len(consumerOptMap)*2+2)
	stream := self.Any.(jetstream.Stream)

	var cfg jetstream.ConsumerConfig
	ctx := context.Background()
	InitConsumerConfig(&cfg, args)

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
		defer cf()
	}
	consumer, err := stream.UpdateConsumer(ctx, cfg)
	if err != nil {
		panic(err)
	}
	return MakeConsumer(consumer)
}

func (caller streamUpdateConsumerCaller) FuncDocs() *slip.FuncDoc {
	return makeConsumerMethodFuncDoc(
		":update-consumer",
		nil,
		"<jet-consumer>",
		`Updates an existing consumer. If consumer does not
exist, an error is raised. A _jet-consumer_ is returned.
`)
}
