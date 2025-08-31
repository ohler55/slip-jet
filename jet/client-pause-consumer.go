// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"
	"time"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type clientPauseConsumerCaller struct{}

func (caller clientPauseConsumerCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":pause-consumer", len(args), 3, 5)
	js := self.Any.(*Client).js

	stream := slip.MustBeString(args[0], "stream")
	consumer := slip.MustBeString(args[1], "consumer")
	until, ok := args[2].(slip.Time)
	if !ok {
		slip.TypePanic(s, 0, "until", args[1], "time")
	}
	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args[3:], slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
		defer cf()
	}
	cpr, err := js.PauseConsumer(ctx, stream, consumer, time.Time(until))
	if err != nil {
		panic(err)
	}
	return slip.DoubleFloat(float64(cpr.PauseRemaining) / float64(time.Second))
}

func (caller clientPauseConsumerCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":pause-consumer",
		Text: `Pauses a consumer until the designated time. The time remaining in seconds is returned.`,
		Args: []*slip.DocArg{
			{
				Name: "stream",
				Type: "string",
				Text: "The stream to remove the consumer from.",
			},
			{
				Name: "consumer",
				Type: "string",
				Text: "The name of the consumer to pause.",
			},
			{
				Name: "until",
				Type: "time",
				Text: "The time to discontinue the pause.",
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
