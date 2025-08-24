// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type clientDeleteConsumerCaller struct{}

func (caller clientDeleteConsumerCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":delete-consumer", len(args), 2, 4)
	js := self.Any.(*Client).js

	stream := slip.MustBeString(args[0], "stream")
	name := slip.MustBeString(args[1], "name")

	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args[2:], slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
		defer cf()
	}
	err := js.DeleteConsumer(ctx, stream, name)
	if err != nil {
		panic(err)
	}
	return nil
}

func (caller clientDeleteConsumerCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":delete-consumer",
		Text: `Removes a consumer with given name from a stream. If consumer does not
exist an error is raised.`,
		Args: []*slip.DocArg{
			{
				Name: "stream",
				Type: "string",
				Text: "The stream to remove the consumer from.",
			},
			{
				Name: "name",
				Type: "string",
				Text: "The consumer name of the consumer to delete.",
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
