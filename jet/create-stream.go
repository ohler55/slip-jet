// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type createStreamCaller struct{}

func (caller createStreamCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":create-stream", len(args), 1, len(streamOptMap)*2+1)
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
	stream, err := js.CreateStream(ctx, cfg)
	if err != nil {
		panic(err)
	}
	fmt.Printf("*** stream: %v\n", stream)

	return MakeStream(stream)
}

func (caller createStreamCaller) Docs() string {
	return makeStreamMethodDoc(":create-stream", "_name_ ", "<jet-stream>",
		"   _name_ [string] the name of the stream.",
		`Creates a new stream with the provided options and returns the created stream.
If a stream with the given name already exists, an error is raised.`)
}
