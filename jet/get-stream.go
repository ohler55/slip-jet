// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type getStreamCaller struct{}

func (caller getStreamCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":get-stream", len(args), 1, 3)
	js := self.Any.(*Client).js

	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args[1:], slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	stream, err := js.Stream(ctx, slip.MustBeString(args[0], "name"))
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil
		}
		panic(err)
	}
	return MakeStream(stream)
}

func (caller getStreamCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":get-stream",
		Text: `Fetches and returns a _jet-stream_ for the given stream name.
If the stream does not exist _nil_ is returned.
`,
		Args: []*slip.DocArg{
			{
				Name: "name",
				Type: "string",
				Text: "The name of the stream to get.",
			},
			{Name: "&key"},
			{
				Name: ":timeout",
				Type: "real",
				Text: `The number of seconds to wait before timing out.`,
			},
		},
		Return: "jet-stream",
	}
}
