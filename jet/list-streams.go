// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type listStreamsCaller struct{}

func (caller listStreamsCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":list-streams", len(args), 0, 4)
	js := self.Any.(*Client).js

	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
		defer cf()
	}
	var opts []jetstream.StreamListOpt
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":subject")); has {
		opts = append(opts, jetstream.WithStreamListSubject(slip.MustBeString(v, "subject")))
	}
	sil := js.ListStreams(ctx, opts...)
	var streams slip.List
	for si := range sil.Info() {
		streams = append(streams, MakeStreamInfo(si))
	}
	return streams
}

func (caller listStreamsCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":list-streams",
		Text: `Returns a list of _jet-stream-info_ instances.`,
		Args: []*slip.DocArg{
			{Name: "&key"},
			{
				Name: ":subject",
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
