// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type deleteStreamCaller struct{}

func (caller deleteStreamCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":delete-stream", len(args), 1, len(streamOptMap)*2+1)
	js := self.Any.(*Client).js

	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args[1:], slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	if err := js.DeleteStream(ctx, slip.MustBeString(args[0], "name")); err != nil {
		panic(err)
	}
	return nil
}

func (caller deleteStreamCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":delete-stream",
		Text: `Removes a stream with given name. If stream does not exist, and error is raised.`,
		Args: []*slip.DocArg{
			{
				Name: "name",
				Type: "string",
				Text: "The name of the stream to delete.",
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
