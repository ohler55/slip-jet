// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type streamDeleteConsumerCaller struct{}

func (caller streamDeleteConsumerCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":delete-consumer", len(args), 1, 3)
	stream := self.Any.(jetstream.Stream)

	name := slip.MustBeString(args[0], "name")
	ctx := context.Background()

	if v, has := slip.GetArgsKeyValue(args[1:], slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
		defer cf()
	}
	err := stream.DeleteConsumer(ctx, name)
	if err != nil {
		panic(err)
	}
	return nil
}

func (caller streamDeleteConsumerCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":delete-consumer",
		Text: `Removes a consumer with given name from a stream. If consumer does not
exist an error is raised.`,
		Args: []*slip.DocArg{
			{
				Name: "name",
				Type: "string",
				Text: `The consumer name of the consumer to delete.`,
			},
			{Name: "&key"},
			{
				Name: ":timeout",
				Type: "real",
				Text: `The number of seconds to wait before timing out.`,
			},
		},
	}
}
