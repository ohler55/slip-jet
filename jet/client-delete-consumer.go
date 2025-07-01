// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type clientDeleteConsumerCaller struct{}

func (caller clientDeleteConsumerCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":delete-consumer", len(args), 2, 4)
	js := self.Any.(*Client).js

	stream := slip.MustBeString(args[0], "stream")
	name := slip.MustBeString(args[1], "name")

	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args[2:], slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	err := js.DeleteConsumer(ctx, stream, name)
	if err != nil {
		panic(err)
	}
	return nil
}

func (caller clientDeleteConsumerCaller) Docs() string {
	return `__:delete-consumer__ _stream_ _name_ &key _timeout_
   _stream_ [string] the stream to remove the consumer from.
   _name_ [string] the consumer name of the consumer to delete.
   _:timeout_ [real] the number of seconds to wait before timing out.


Removes a consumer with given name from a stream. If consumer does not
exist an error is raised.
`
}
