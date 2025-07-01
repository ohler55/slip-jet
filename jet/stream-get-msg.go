// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type streamGetMsgCaller struct{}

func (caller streamGetMsgCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":get-msg", len(args), 1, 7)
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
	var opts []jetstream.GetMsgOpt
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":subject")); has {
		opts = append(opts, jetstream.WithGetMsgSubject(slip.MustBeString(v, ":subject")))
	}
	raw, err := stream.GetMsg(ctx, uint64(seq), opts...)
	if err != nil {
		panic(err)
	}
	pm := PubMsg{
		Meta: &jetstream.MsgMetadata{
			Sequence:  jetstream.SequencePair{Stream: raw.Sequence},
			Timestamp: raw.Time,
		},
		Body: raw.Data,
		Head: raw.Header,
		Subj: raw.Subject,
	}
	return MakeMsg(&pm)
}

func (caller streamGetMsgCaller) Docs() string {
	return `__:get-msg__ _sequence_ &key _timeout_ _subject_ => _jet-msg_
   _sequence_ [fixnum] the sequence number of the message to get.
   _:timeout_ [real] the number of seconds to wait before timing out.
   _:subject_ [string] sets the stream subject from which the message should be
retrieved. Server will return a first message with a seq >= to the input seq that has
the specified subject.


Retrieves a message stored in JetStream by sequence number.
`
}
