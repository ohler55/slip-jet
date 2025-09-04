// Copyright (c) 2025, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type streamCreateOrUpdatePushConsumerCaller struct{}

func (caller streamCreateOrUpdatePushConsumerCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":create-or-update-push-consumer", len(args), 0, len(consumerOptMap)*2+2)
	stream := self.Any.(jetstream.Stream)

	var cfg jetstream.ConsumerConfig
	ctx := context.Background()
	InitConsumerConfig(&cfg, args)

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
		defer cf()
	}
	consumer, err := stream.CreateOrUpdatePushConsumer(ctx, cfg)
	if err != nil {
		panic(err)
	}
	return MakePushConsumer(consumer)
}

func (caller streamCreateOrUpdatePushConsumerCaller) FuncDocs() *slip.FuncDoc {
	return makeConsumerMethodFuncDoc(
		":create-or-update-push-consumer",
		nil,
		"<jet-push-consumer>",
		`Create a push consumer on a given stream with given config. If consumer already
exists, it will be updated (if possible). A _jet-push-consumer_ is returned, allowing to
operations on a consumer (e.g. fetch messages).
`)
}
