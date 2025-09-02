// Copyright (c) 2025, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type streamCreatePushConsumerCaller struct{}

func (caller streamCreatePushConsumerCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":create-push-consumer", len(args), 0, len(consumerOptMap)*2+2)
	stream := self.Any.(jetstream.Stream)

	var cfg jetstream.ConsumerConfig
	ctx := context.Background()
	InitConsumerConfig(&cfg, args)

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
		defer cf()
	}
	consumer, err := stream.CreatePushConsumer(ctx, cfg)
	if err != nil {
		panic(err)
	}
	return MakePushConsumer(consumer)
}

func (caller streamCreatePushConsumerCaller) FuncDocs() *slip.FuncDoc {
	return makeConsumerMethodFuncDoc(
		":create-push-consumer",
		nil,
		"<jet-push-consumer>",
		`creates a push consumer on a given stream with given
config. If consumer already exists and the provided configuration
differs from its configuration, ErrConsumerExists is returned. If the
provided configuration is the same as the existing consumer, the
existing consumer is returned. Consumer interface is returned,
allowing to consume messages.
`)
}
