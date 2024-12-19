// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type clientGetConsumerCaller struct{}

func (caller clientGetConsumerCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":get-consumer", len(args), 2, 4)
	js := self.Any.(*Client).js

	stream := slip.MustBeString(args[0], "stream")
	name := slip.MustBeString(args[1], "name")

	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args[2:], slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	consumer, err := js.Consumer(ctx, stream, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			return nil
		}
		panic(err)
	}
	return MakeConsumer(consumer)
}

func (caller clientGetConsumerCaller) Docs() string {
	return `__:get-consumer__ _stream_ _name_ &key _timeout_ => _jet-consumer_
   _stream_ [string] the stream to search for the consumer in.
   _name_ [string] the consumer name of the consumer to get.
   _:timeout_ [real] the number of seconds to wait before timing out.


Returns a _jet-consumer_ for an existing consumer, allowing processing
of messages. If consumer does not exist, _nil_ is returned.
`
}
