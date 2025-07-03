// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type publishCaller struct{}

func (caller publishCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":publish", len(args), 1, 20)
	js := self.Any.(*Client).js
	ctx := context.Background()
	var (
		opts []jetstream.PublishOpt
		msg  nats.Msg
	)
	switch ta := args[0].(type) {
	case slip.Octets:
		msg.Data = []byte(ta)
	case slip.String:
		msg.Data = []byte(ta)
	case *flavors.Instance:
		if ta.IsA(msgFlavor) {
			pm := ta.Any.(*PubMsg)
			msg.Subject = pm.Subj
			msg.Reply = pm.Repl
			msg.Header = pm.Head
			msg.Data = pm.Body
		} else {
			slip.PanicType("payload", ta, "octets", "string", "jet-msg instance")
		}
	default:
		slip.PanicType("payload", ta, "octets", "string", "jet-msg instance")
	}
	args = args[1:]
	if 0 < len(args) {
		if ss, ok := args[0].(slip.String); ok {
			msg.Subject = string(ss)
			args = args[1:]
		}
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		var cf context.CancelFunc
		ctx, cf = context.WithTimeout(ctx, mustBeDuration(v, ":timeout"))
		defer cf()
	}
	opts = pubOptsFromArgs(opts, args)
	pa, err := js.PublishMsg(ctx, &msg, opts...)
	if err != nil {
		panic(err)
	}
	return MakeAck(pa.Stream, pa.Sequence, pa.Duplicate, pa.Domain)
}

func (caller publishCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":publish",
		Text: `Performs a synchronous publish to a stream and waits for an ack from
server. It accepts a message payload and optional subject name (which must be
bound to a stream) which can be _octets_, _string_, or a _jet-msg_
instance. If the _payload_ is not a _jet-msg_ instance then the subject must
be provided. Multiple options in the form of keywords and values are
supported. An instance of the _jet-ack_ flavor is returned with information
about the published message.`,
		Args: []*slip.DocArg{
			{
				Name: "payload",
				Type: "octets|string|jet-msg",
				Text: "Data to publish as the content of a message.",
			},
			{Name: "&optional"},
			{
				Name: "subject",
				Type: "string",
				Text: `Subject to publish the message on. The subject must be bound to a stream.`,
			},
			{Name: "&key"},
			{
				Name: ":timeout",
				Type: "real",
				Text: `Timeout in seconds for the publish.`,
			},
			{
				Name: ":expect-last-msg-id",
				Type: "fixnum",
				Text: `Sets the expected message ID the last message on a stream
should have. If the last message has a different message ID server will reject the message
and publish will fail.`,
			},
			{
				Name: ":expect-last-sequence",
				Type: "fixnum",
				Text: `Sets the expected sequence number the last message on a
stream should have. If the last message has a different sequence number server will reject
the message and publish will fail.`,
			},
			{
				Name: ":expect-last-subject-sequence",
				Type: "fixnum",
				Text: `Sets the expected sequence number the last message
on a subject the message is published to. If the last message on a subject has a different
sequence number server will reject the message and publish will fail.`,
			},
			{
				Name: ":expect-stream",
				Type: "string",
				Text: `Sets the expected stream the message should be published to. If
the message is published to a different stream server will reject the message and publish will
fail.`,
			},
			{
				Name: ":msg-id",
				Type: "fixnum",
				Text: `Sets the message ID used for deduplication.`,
			},
			{
				Name:    ":retry-attempts",
				Type:    "fixnum",
				Text:    `Sets the retry number of attempts when ErrNoResponders is encountered.`,
				Default: slip.Fixnum(2),
			},
			{
				Name:    ":retry-wait",
				Type:    "real",
				Text:    `Sets the retry wait time in seconds when ErrNoResponders is encountered.`,
				Default: slip.DoubleFloat(0.25),
			},
		},
		Return: "jet-ack",
	}
}

func mustBeInt(arg slip.Object, name string) (i int) {
	if num, ok := arg.(slip.Fixnum); ok {
		i = int(num)
	} else {
		slip.PanicType(name, arg, "fixnum")
	}
	return
}

func mustBeDuration(arg slip.Object, name string) (dur time.Duration) {
	if num, ok := arg.(slip.Real); ok {
		dur = time.Duration(num.RealValue() * float64(time.Second))
	} else {
		slip.PanicType(name, arg, "real")
	}
	return
}

func pubOptsFromArgs(opts []jetstream.PublishOpt, args slip.List) []jetstream.PublishOpt {
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":expect-last-msg-id")); has {
		opts = append(opts, jetstream.WithExpectLastMsgID(slip.MustBeString(v, ":expect-last-msg-id")))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":expect-stream")); has {
		opts = append(opts, jetstream.WithExpectStream(slip.MustBeString(v, ":expect-stream")))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":msg-id")); has {
		opts = append(opts, jetstream.WithMsgID(slip.MustBeString(v, ":msg-id")))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":expect-last-sequence")); has {
		opts = append(opts, jetstream.WithExpectLastSequence(uint64(mustBeInt(v, ":expect-last-sequence"))))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":expect-last-subject-sequence")); has {
		opts = append(opts,
			jetstream.WithExpectLastSequencePerSubject(uint64(mustBeInt(v, ":expect-last-subject-sequence"))))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":retry-attempts")); has {
		opts = append(opts, jetstream.WithRetryAttempts(mustBeInt(v, ":retry-attempts")))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":retry-wait")); has {
		opts = append(opts, jetstream.WithRetryWait(mustBeDuration(v, ":retry-wait")))
	}
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":stall-wait")); has {
		opts = append(opts, jetstream.WithStallWait(mustBeDuration(v, ":stall-wait")))
	}
	return opts
}
