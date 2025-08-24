// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type streamDeleteMsgCaller struct{}

func (caller streamDeleteMsgCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":delete-msg", len(args), 1, 7)
	stream := self.Any.(jetstream.Stream)
	seq, ok := args[0].(slip.Fixnum)
	if !ok {
		slip.TypePanic(s, depth, ":sequence", args[0], "fixnum")
	}
	args = args[1:]
	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
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

func (caller streamDeleteMsgCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":delete-msg",
		Text: `Deletes a message from a stream. On the server, the message is marked as erased,
but not overwritten unless the _:secure_ option is true.`,
		Args: []*slip.DocArg{
			{
				Name: "sequence",
				Type: "fixnum",
				Text: `The sequence number of the message to delete.`,
			},
			{Name: "&key"},
			{
				Name: ":timeout",
				Type: "real",
				Text: `The number of seconds to wait before timing out.`,
			},
			{
				Name: ":secure",
				Type: "boolean",
				Text: `deletes a message from a stream. The deleted message
is overwritten with random data. As a result, this operation is slower
than when _:secure_ if false.`,
			},
		},
	}
}
