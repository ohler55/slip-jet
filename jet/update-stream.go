// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type updateStreamCaller struct{}

func (caller updateStreamCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":update-stream", len(args), 1, len(streamOptMap)*2+1)
	js := self.Any.(*Client).js

	var cfg jetstream.StreamConfig
	cfg.Name = slip.MustBeString(args[0], "name")

	args = args[1:]
	ctx := context.Background()
	InitStreamConfig(&cfg, args)

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	stream, err := js.UpdateStream(ctx, cfg)
	if err != nil {
		panic(err)
	}
	return MakeStream(stream)
}

func (caller updateStreamCaller) Docs() string {
	return makeStreamMethodDoc(":update-stream", "_name_ ", "<jet-stream>",
		"   _name_ [string] the name of the stream.",
		`Updates an existing stream. If stream does not exist, and error is raised.`)
}
