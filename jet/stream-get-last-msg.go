// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go/jetstream"
)

type streamGetLastMsgCaller struct{}

func (caller streamGetLastMsgCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.MethodArgCountCheck(s, depth, self, ":get-last-msg", len(args), 1, 7)
	stream := self.Any.(jetstream.Stream)
	subject := slip.MustBeString(args[0], "subject")
	args = args[1:]
	ctx := context.Background()
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(s, v, ":timeout", depth))
		defer cf()
	}
	raw, err := stream.GetLastMsgForSubject(ctx, subject)
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

func (caller streamGetLastMsgCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":get-last-msg",
		Text: `Retrieves the last stream message stored in JetStream on a given subject subject.`,
		Args: []*slip.DocArg{
			{
				Name: "subject",
				Type: "string",
				Text: "The subject to get the last message of.",
			},
			{Name: "&key"},
			{
				Name: ":timeout",
				Type: "real",
				Text: "the number of seconds to wait before timing out.",
			},
		},
		Return: "jet-msg",
	}
}
