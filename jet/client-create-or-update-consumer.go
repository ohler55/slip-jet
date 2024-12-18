// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type clientCreateOrUpdateConsumerCaller struct{}

func (caller clientCreateOrUpdateConsumerCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":create-consumer", len(args), 1, len(consumerOptMap)*2+3)
	js := self.Any.(*Client).js

	stream := slip.MustBeString(args[0], "stream")

	args = args[1:]
	ctx := context.Background()

	var cfg jetstream.ConsumerConfig
	InitConsumerConfig(&cfg, args)

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	consumer, err := js.CreateOrUpdateConsumer(ctx, stream, cfg)
	if err != nil {
		panic(err)
	}
	return MakeConsumer(consumer)
}

func (caller clientCreateOrUpdateConsumerCaller) Docs() string {
	return makeConsumerMethodDoc(":create-or-update-consumer", "_stream_ ", "<jet-consumer>",
		"   _stream_ [string] the name of the stream.",
		`Creates a consumer on a given stream with given
config. If consumer already exists, it will be updated (if possible) otherwise
a _jet-consumer_ is returned, allowing operations on a
consumer (e.g. fetch messages).
`)
}
