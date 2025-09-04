// Copyright (c) 2025, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type streamUnpinConsumerCaller struct{}

func (caller streamUnpinConsumerCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":unpin-consumer", len(args), 2, 4)
	stream := self.Any.(jetstream.Stream)

	consumer := slip.MustBeString(args[0], "consumer")
	group := slip.MustBeString(args[1], "group")
	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args[2:], slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
		defer cf()
	}
	if err := stream.UnpinConsumer(ctx, consumer, group); err != nil {
		panic(err)
	}
	return nil
}

func (caller streamUnpinConsumerCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":unpin-consumer",
		Text: `Unpins the currently pinned client for a consumer for the given group name.
If consumer does not exist, an error is raised.`,
		Args: []*slip.DocArg{
			{
				Name: "consumer",
				Type: "string",
				Text: "The name of the consumer to unpin.",
			},
			{
				Name: "group",
				Type: "string",
				Text: `The group the consumer is in.`,
			},
			{Name: "&key"},
			{
				Name: ":timeout",
				Type: "real",
				Text: `The number of seconds to wait before timing out.`,
			},
		},
		Return: "nil",
	}
}
