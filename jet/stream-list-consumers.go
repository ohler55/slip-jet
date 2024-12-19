// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type streamListConsumersCaller struct{}

func (caller streamListConsumersCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":list-consumers", len(args), 0, 2)
	stream := self.Any.(jetstream.Stream)

	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	lister := stream.ListConsumers(ctx)
	var list slip.List
	for ci := range lister.Info() {
		list = append(list, MakeConsumerInfo(ci))
	}
	if err := lister.Err(); err != nil {
		panic(err)
	}
	return list
}

func (caller streamListConsumersCaller) Docs() string {
	return `__:list-consumers__ &key _timeout_ => _list_
   _:timeout_ [real] the number of seconds to wait before timing out.


Returns a list of _jet-consumer-info_ instances.
`
}
