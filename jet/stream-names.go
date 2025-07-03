// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type streamNamesCaller struct{}

func (caller streamNamesCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":stream-names", len(args), 0, 4)
	js := self.Any.(*Client).js

	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	var opts []jetstream.StreamListOpt
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":subject")); has {
		opts = append(opts, jetstream.WithStreamListSubject(slip.MustBeString(v, "subject")))
	}
	snl := js.StreamNames(ctx, opts...)
	var names slip.List
	for name := range snl.Name() {
		names = append(names, slip.String(name))
	}
	return names
}

func (caller streamNamesCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":stream-names",
		Text: `Returns a list of stream names.`,
		Args: []*slip.DocArg{
			{Name: "&key"},
			{
				Name: "subject",
				Type: "string",
				Text: `Used to filter results to only streams that have the
given subject in their configuration.`,
			},
			{
				Name: ":timeout",
				Type: "real",
				Text: `The number of seconds to wait before timing out.`,
			},
		},
		Return: "list",
	}
}
