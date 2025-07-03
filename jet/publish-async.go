// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type publishAsyncCaller struct{}

func (caller publishAsyncCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":publish-async", len(args), 1, 20)
	js := self.Any.(*Client).js
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
	opts = pubOptsFromArgs(opts, args)
	paf, err := js.PublishMsgAsync(&msg, opts...)
	if err != nil {
		panic(err)
	}
	return MakeAckFuture(paf)
}

func (caller publishAsyncCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":publish-async",
		Text: `Performs a publish to a stream and returns a _jet-ack-future_, not blocking
while waiting for an acknowledgement. It accepts a message payload and
optional subject name (which must be bound to a stream) which can be _octets_,
_string_, or a _jet-msg_ instance. If the _payload_ is not a _jet-msg_
instance then the subject must be provided. Multiple options in the form of
keywords and values are supported. A _jet-ack-future_ instance is returned.


PublishMsgAsync does not guarantee that the message has been
sent to the server and thus messages can be stored in the stream
received by the server.`,
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
			{
				Name: ":stall-wait",
				Type: "real",
				Text: `Sets the max wait when the producer becomes stall producing messages.
If a publish call is blocked for this long, ErrTooManyStalledMsgs is returned.`,
			},
		},
		Return: "jet-ack",
	}
}
