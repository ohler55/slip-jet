// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type clientCreateConsumerCaller struct{}

func (caller clientCreateConsumerCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":create-consumer", len(args), 1, len(consumerOptMap)*2+3)
	js := self.Any.(*Client).js

	stream := slip.MustBeString(args[0], "stream")

	args = args[1:]
	ctx := context.Background()

	var cfg jetstream.ConsumerConfig
	InitConsumerConfig(&cfg, args)

	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
		defer cf()
	}
	consumer, err := js.CreateConsumer(ctx, stream, cfg)
	if err != nil {
		panic(err)
	}
	return MakeConsumer(consumer)
}

func (caller clientCreateConsumerCaller) FuncDocs() *slip.FuncDoc {
	return makeConsumerMethodFuncDoc(
		":create-consumer",
		&slip.DocArg{Name: "stream", Type: "string", Text: "The name of the stream."},
		"<jet-consumer>",
		`Creates a consumer on a given stream with given
config. If consumer already exists and the provided configuration
differs from its configuration, an error is raised. If the
provided configuration is the same as the existing consumer, the
existing consumer is returned. Otherwise a new _jet-consumer_ is returned,
allowing operations on a consumer (e.g. fetch messages).
`)
}
