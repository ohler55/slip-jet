// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type streamCreateConsumerCaller struct{}

func (caller streamCreateConsumerCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":create-consumer", len(args), 0, len(consumerOptMap)*2+2)
	stream := self.Any.(jetstream.Stream)

	var cfg jetstream.ConsumerConfig
	ctx := context.Background()
	InitConsumerConfig(&cfg, args)

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	consumer, err := stream.CreateConsumer(ctx, cfg)
	if err != nil {
		panic(err)
	}
	return MakeConsumer(consumer)
}

func (caller streamCreateConsumerCaller) Docs() string {
	return makeConsumerMethodDoc(":create-consumer", "", "<jet-stream>", "",
		`Creates a consumer on a given stream with given config. If consumer already exists
and the provided configuration differs from its configuration, an error is raised is returned.
If the provided configuration is the same as the existing consumer, the existing consumer is
returned. Consumer interface is returned, allowing to operate on a consumer (e.g. fetch messages).
`)
}
