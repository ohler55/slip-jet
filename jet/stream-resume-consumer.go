// Copyright (c) 2025, Peter Ohler, All rights reserved.

package jet

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type streamResumeConsumerCaller struct{}

func (caller streamResumeConsumerCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":resume-consumer", len(args), 1, 3)
	stream := self.Any.(jetstream.Stream)

	consumer := slip.MustBeString(args[0], "consumer")
	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args[1:], slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
		defer cf()
	}
	cpr, err := stream.ResumeConsumer(ctx, consumer)
	if err != nil {
		panic(err)
	}
	return slip.DoubleFloat(float64(cpr.PauseRemaining) / float64(time.Second))
}

func (caller streamResumeConsumerCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":resume-consumer",
		Text: `Resumes a consumer. The time remaining in seconds is returned.`,
		Args: []*slip.DocArg{
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
		Return: "real",
	}
}
