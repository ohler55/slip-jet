// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type clientResumeConsumerCaller struct{}

func (caller clientResumeConsumerCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":resume-consumer", len(args), 2, 4)
	js := self.Any.(*Client).js

	stream := slip.MustBeString(args[0], "stream")
	consumer := slip.MustBeString(args[1], "consumer")

	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args[2:], slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
		defer cf()
	}
	if _, err := js.ResumeConsumer(ctx, stream, consumer); err != nil {
		panic(err)
	}
	return nil
}

func (caller clientResumeConsumerCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":resume-consumer",
		Text: `Resumes a consumer until the designated time. The time remaining in seconds is returned.`,
		Args: []*slip.DocArg{
			{
				Name: "stream",
				Type: "string",
				Text: "The stream to remove the consumer from.",
			},
			{
				Name: "consumer",
				Type: "string",
				Text: "The name of the consumer to resume.",
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
