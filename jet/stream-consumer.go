// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type streamConsumerCaller struct{}

func (caller streamConsumerCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":consumer", len(args), 1, 3)
	stream := self.Any.(jetstream.Stream)

	name := slip.MustBeString(args[0], "name")
	ctx := context.Background()

	if v, has := slip.GetArgsKeyValue(args[1:], slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
		defer cf()
	}
	consumer, err := stream.Consumer(ctx, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			return nil
		}
		panic(err)
	}
	return MakeConsumer(consumer)
}

func (caller streamConsumerCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":consumer",
		Text: `Returns a _jet-consumer_ for an existing consumer, allowing processing
of messages. If consumer does not exist, _nil_ is returned.`,
		Args: []*slip.DocArg{
			{
				Name: "name",
				Type: "string",
				Text: `The consumer name of the consumer to get.`,
			},
			{Name: "&key"},
			{
				Name: ":timeout",
				Type: "real",
				Text: `The number of seconds to wait before timing out.`,
			},
		},
		Return: "jet-consumer",
	}
}
