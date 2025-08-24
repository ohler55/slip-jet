// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type streamGetMsgCaller struct{}

func (caller streamGetMsgCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":get-msg", len(args), 1, 7)
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

func (caller streamGetMsgCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":get-msg",
		Text: `Retrieves a message stored in JetStream by sequence number.`,
		Args: []*slip.DocArg{
			{
				Name: "sequence",
				Type: "fixnum",
				Text: "The sequence number of the message to get.",
			},
			{Name: "&key"},
			{
				Name: ":timeout",
				Type: "real",
				Text: "The number of seconds to wait before timing out.",
			},
			{
				Name: ":subject",
				Type: "string",
				Text: `Sets the stream subject from which the message should be
retrieved. Server will return a first message with a seq >= to the input seq that has
the specified subject.`,
			},
		},
		Return: "jet-msg",
	}
}
