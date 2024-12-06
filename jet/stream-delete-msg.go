// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type streamDeleteMsgCaller struct{}

func (caller streamDeleteMsgCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.CheckMethodArgCount(self, ":delete-msg", len(args), 1, 7)
	stream := self.Any.(jetstream.Stream)
	seq, ok := args[0].(slip.Fixnum)
	if !ok {
		slip.PanicType(":sequence", args[0], "fixnum")
	}
	args = args[1:]
	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	var err error
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":secure")); has && v != nil {
		err = stream.SecureDeleteMsg(ctx, uint64(seq))
	} else {
		err = stream.DeleteMsg(ctx, uint64(seq))
	}
	if err != nil {
		panic(err)
	}
	return nil
}

func (caller streamDeleteMsgCaller) Docs() string {
	return `__:delete-msg__ _sequence_ &key _timeout_ _secure_
   _sequence_ [fixnum] the sequence number of the message to delete.
   _:timeout_ [real] the number of seconds to wait before timing out.
   _:secure_ [boolean] deletes a message from a stream. The deleted message
is overwritten with random data. As a result, this operation is slower
than when _:secure_ if false.


Deletes a message from a stream. On the server, the message is marked as erased,
but not overwritten unless the _:secure_ option is true.
`
}
