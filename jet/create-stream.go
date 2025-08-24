// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type createStreamCaller struct{}

func (caller createStreamCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":create-stream", len(args), 1, len(streamOptMap)*2+3)
	js := self.Any.(*Client).js

	var cfg jetstream.StreamConfig
	cfg.Name = slip.MustBeString(args[0], "name")

	args = args[1:]
	ctx := context.Background()
	InitStreamConfig(&cfg, args)

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
		defer cf()
	}
	stream, err := js.CreateStream(ctx, cfg)
	if err != nil {
		panic(err)
	}
	return MakeStream(stream)
}

func (caller createStreamCaller) FuncDocs() *slip.FuncDoc {
	return makeStreamMethodFuncDoc(
		":create-stream",
		&slip.DocArg{Name: "name", Type: "string", Text: "The name of the stream."},
		"<jet-stream>",
		`Creates a new stream with the provided options and returns the created stream.
If a stream with the given name already exists, an error is raised.`)
}
