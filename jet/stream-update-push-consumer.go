// Copyright (c) 2025, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type streamUpdatePushConsumerCaller struct{}

func (caller streamUpdatePushConsumerCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":update-push-consumer", len(args), 0, len(consumerOptMap)*2+2)
	stream := self.Any.(jetstream.Stream)

	var cfg jetstream.ConsumerConfig
	ctx := context.Background()
	InitConsumerConfig(&cfg, args)

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
		defer cf()
	}
	consumer, err := stream.UpdatePushConsumer(ctx, cfg)
	if err != nil {
		panic(err)
	}
	return MakePushConsumer(consumer)
}

func (caller streamUpdatePushConsumerCaller) FuncDocs() *slip.FuncDoc {
	return makeConsumerMethodFuncDoc(
		":update-push-consumer",
		nil,
		"<jet-consumer>",
		`Updates an existing consumer. If consumer does not
exist, an error is raised. A _jet-consumer_ is returned.
`)
}
