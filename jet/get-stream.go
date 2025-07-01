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

func (caller getStreamCaller) Docs() string {
	return `__:get-stream__ _name_ &key _timeout_ => _jet-stream__
   _name_ [string] name of the stream to get.
   _:timeout_ [real] the number of seconds to wait before timing out.


Fetches and returns a _jet-stream_ for the given stream name.
If the stream does not exist _nil_ is returned.
`
}
