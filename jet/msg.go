// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"
	"strings"
	"time"

	"github.com/ohler55/ojg/oj"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/bag"
	"github.com/ohler55/slip/pkg/flavors"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	msgFlavor *flavors.Flavor
)

func defMsg() {
	msgFlavor = flavors.DefFlavor("jet-msg",
		map[string]slip.Object{},
		[]string{},
		slip.List{
			slip.List{
				slip.Symbol(":init-keywords"),
				slip.Symbol(":subject"),
				slip.Symbol(":reply"),
				slip.Symbol(":data"),
				slip.Symbol(":headers"),
			},
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`
Includes methods for accessing information in a message. Methods for ack and nak are also included.

`),
			},
		},
		&Pkg,
	)
	msgFlavor.DefMethod(":init", "", msgInitCaller(true))

	msgFlavor.DefMethod(":consumer-sequence", "", msgConsumerSequenceCaller{})
	flavors.FlosFun("jet-msg-consumer-sequence", ":consumer-sequence", msgConsumerSequenceCaller{}.FuncDocs(), &Pkg)

	msgFlavor.DefMethod(":stream-sequence", "", msgStreamSequenceCaller{})
	flavors.FlosFun("jet-msg-stream-sequence", ":stream-sequence", msgStreamSequenceCaller{}.FuncDocs(), &Pkg)

	msgFlavor.DefMethod(":number-delivered", "", msgNumberDeliveredCaller{})
	flavors.FlosFun("jet-msg-number-delivered", ":number-delivered", msgNumberDeliveredCaller{}.FuncDocs(), &Pkg)

	msgFlavor.DefMethod(":number-pending", "", msgNumberPendingCaller{})
	flavors.FlosFun("jet-msg-number-pending", ":number-pending", msgNumberPendingCaller{}.FuncDocs(), &Pkg)

	msgFlavor.DefMethod(":timestamp", "", msgTimestampCaller{})
	flavors.FlosFun("jet-msg-timestamp", ":number-pending", msgTimestampCaller{}.FuncDocs(), &Pkg)

	msgFlavor.DefMethod(":stream", "", msgStreamCaller{})
	flavors.FlosFun("jet-msg-stream", ":stream", msgStreamCaller{}.FuncDocs(), &Pkg)

	msgFlavor.DefMethod(":consumer", "", msgConsumerCaller{})
	flavors.FlosFun("jet-msg-consumer", ":consumer", msgConsumerCaller{}.FuncDocs(), &Pkg)

	msgFlavor.DefMethod(":domain", "", msgDomainCaller{})
	flavors.FlosFun("jet-msg-domain", ":domain", msgDomainCaller{}.FuncDocs(), &Pkg)

	msgFlavor.DefMethod(":data", "", msgDataCaller{})
	flavors.FlosFun("jet-msg-data", ":data", msgDataCaller{}.FuncDocs(), &Pkg)

	msgFlavor.DefMethod(":headers", "", msgHeadersCaller{})
	flavors.FlosFun("jet-msg-headers", ":headers", msgHeadersCaller{}.FuncDocs(), &Pkg)

	msgFlavor.DefMethod(":subject", "", msgSubjectCaller{})
	flavors.FlosFun("jet-msg-subject", ":subject", msgSubjectCaller{}.FuncDocs(), &Pkg)

	msgFlavor.DefMethod(":reply", "", msgReplyCaller{})
	flavors.FlosFun("jet-msg-reply", ":reply", msgReplyCaller{}.FuncDocs(), &Pkg)

	msgFlavor.DefMethod(":ack", "", msgAckCaller{})
	flavors.FlosFun("jet-msg-ack", ":ack", msgAckCaller{}.FuncDocs(), &Pkg)

	msgFlavor.DefMethod(":nak", "", msgNakCaller{})
	flavors.FlosFun("jet-msg-nak", ":nak", msgNakCaller{}.FuncDocs(), &Pkg)

	msgFlavor.DefMethod(":in-progress", "", msgInProgressCaller{})
	flavors.FlosFun("jet-msg-in-progress", ":in-progress", msgInProgressCaller{}.FuncDocs(), &Pkg)

	msgFlavor.DefMethod(":term", "", msgTermCaller{})
	flavors.FlosFun("jet-msg-term", ":term", msgTermCaller{}.FuncDocs(), &Pkg)
}

// MakeMsg makes a new jet-msg instance.
func MakeMsg(m jetstream.Msg) (inst *flavors.Instance) {
	if m == nil {
		m = &PubMsg{}
	}
	inst = msgFlavor.MakeInstance().(*flavors.Instance)
	inst.Any = m
	return
}

type msgInitCaller bool

func (caller msgInitCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	if 0 < len(args) {
		args = args[0].(slip.List)
	}
	var pm PubMsg
	for i := 0; i < len(args); i += 2 {
		key, _ := args[i].(slip.Symbol)
		k := string(key)
		switch {
		case strings.EqualFold(":subject", k):
			pm.Subj = getStrArg(s, args[i+1], k, depth)
		case strings.EqualFold(":reply", k):
			pm.Repl = getStrArg(s, args[i+1], k, depth)
		case strings.EqualFold(":headers", k):
			pm.Head = assocToHeader(s, args[i+1], ":headers", depth)
		case strings.EqualFold(":data", k):
			switch td := args[i+1].(type) {
			case slip.Octets:
				pm.Body = []byte(td)
			case slip.String:
				pm.Body = []byte(td)
			case *flavors.Instance:
				if td.Class() == bag.Flavor() {
					pm.Body = []byte(oj.JSON(td.Any))
				} else {
					slip.TypePanic(s, depth, ":data", td, "octets", "string", "bag instance")
				}
			default:
				slip.TypePanic(s, depth, ":data", args[i+1], "octets", "string", "bag instance")
			}
		}
	}
	if self.Any == nil {
		self.Any = &pm
	}
	return nil
}

func (caller msgInitCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":init",
		Text: `Sets the initial values when _make-instance_ is called.`,
		Args: []*slip.DocArg{
			{Name: "&key"},
			{
				Name: ":subject",
				Type: "string",
				Text: `Subject to publish the message on.`,
			},
			{
				Name: ":reply",
				Type: "string",
				Text: `Reply subject to set in the message.`,
			},
			{
				Name: ":headers",
				Type: "assoc",
				Text: `An association list with the values as a list such as (("Something" "str1" "str2"))`,
			},
			{
				Name: ":data",
				Type: "octets|string|bag instance",
				Text: `The body or payload of the message. If a _bag_ instance then the
content will be serialized into JSON.`,
			},
		},
	}
}

type msgConsumerSequenceCaller struct{}

func (caller msgConsumerSequenceCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	meta, err := self.Any.(jetstream.Msg).Metadata()
	if err != nil {
		panic(err)
	}
	return slip.Fixnum(meta.Sequence.Consumer)
}

func (caller msgConsumerSequenceCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":consumer-sequence",
		Text:   `Returns the consumer sequence for the message.`,
		Return: "fixnum",
	}
}

type msgStreamSequenceCaller struct{}

func (caller msgStreamSequenceCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	meta, err := self.Any.(jetstream.Msg).Metadata()
	if err != nil {
		panic(err)
	}
	return slip.Fixnum(meta.Sequence.Stream)
}

func (caller msgStreamSequenceCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":stream-sequence",
		Text:   `Returns the stream sequence for the message.`,
		Return: "fixnum",
	}
}

type msgNumberDeliveredCaller struct{}

func (caller msgNumberDeliveredCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	meta, err := self.Any.(jetstream.Msg).Metadata()
	if err != nil {
		panic(err)
	}
	return slip.Fixnum(meta.NumDelivered)
}

func (caller msgNumberDeliveredCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":number-delivered",
		Text:   `Returns the number of times the associated message was delivered to the consumer.`,
		Return: "fixnum",
	}
}

type msgNumberPendingCaller struct{}

func (caller msgNumberPendingCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	meta, err := self.Any.(jetstream.Msg).Metadata()
	if err != nil {
		panic(err)
	}
	return slip.Fixnum(meta.NumPending)
}

func (caller msgNumberPendingCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":number-pending",
		Text:   `Returns the number of messages pending that match the consumer's filter.`,
		Return: "fixnum",
	}
}

type msgTimestampCaller struct{}

func (caller msgTimestampCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	meta, err := self.Any.(jetstream.Msg).Metadata()
	if err != nil {
		panic(err)
	}
	return slip.Time(meta.Timestamp)
}

func (caller msgTimestampCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":timestamp",
		Text:   `Returns the time the message was originally stored on a stream.`,
		Return: "time",
	}
}

type msgStreamCaller struct{}

func (caller msgStreamCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	meta, err := self.Any.(jetstream.Msg).Metadata()
	if err != nil {
		panic(err)
	}
	return slip.String(meta.Stream)
}

func (caller msgStreamCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":stream",
		Text:   `Returns the stream name this message is stored on.`,
		Return: "string",
	}
}

type msgConsumerCaller struct{}

func (caller msgConsumerCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	meta, err := self.Any.(jetstream.Msg).Metadata()
	if err != nil {
		panic(err)
	}
	return slip.String(meta.Consumer)
}

func (caller msgConsumerCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":consumer",
		Text:   `Returns the consumer name this message was delivered to.`,
		Return: "string",
	}
}

type msgDomainCaller struct{}

func (caller msgDomainCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	meta, err := self.Any.(jetstream.Msg).Metadata()
	if err != nil {
		panic(err)
	}
	return slip.String(meta.Domain)
}

func (caller msgDomainCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":domain",
		Text:   `Returns the domain name this message was received on.`,
		Return: "string",
	}
}

type msgDataCaller struct{}

func (caller msgDataCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)

	return slip.Octets(self.Any.(jetstream.Msg).Data())
}

func (caller msgDataCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":data",
		Text:   `Returns the data for a message.`,
		Return: "octets",
	}
}

type msgHeadersCaller struct{}

func (caller msgHeadersCaller) Call(s *slip.Scope, args slip.List, _ int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	headers := self.Any.(jetstream.Msg).Headers()
	if 0 < len(headers) {
		list := make(slip.List, 0, len(headers))
		for k, sa := range headers {
			el := make(slip.List, len(sa)+1)
			el[0] = slip.String(k)
			for i, v := range sa {
				el[i+1] = slip.String(v)
			}
			list = append(list, el)
		}
		result = list
	}
	return
}

func (caller msgHeadersCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":headers",
		Text:   `Returns the headers for a message.`,
		Return: "list",
	}
}

type msgSubjectCaller struct{}

func (caller msgSubjectCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)

	return slip.String(self.Any.(jetstream.Msg).Subject())
}

func (caller msgSubjectCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":subject",
		Text:   `Returns the subject the message was published and received on.`,
		Return: "string",
	}
}

type msgReplyCaller struct{}

func (caller msgReplyCaller) Call(s *slip.Scope, args slip.List, _ int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	if 0 < len(self.Any.(jetstream.Msg).Reply()) {
		result = slip.String(self.Any.(jetstream.Msg).Reply())
	}
	return
}

func (caller msgReplyCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":reply",
		Text:   `Returns the subject the message should reply on or _nil_ if none has been specified.`,
		Return: "string",
	}
}

type msgAckCaller struct{}

func (caller msgAckCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":ack", len(args), 0, 2)
	var (
		timeout time.Duration
		err     error
	)
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":timeout")); has {
		if num, ok := v.(slip.Real); ok {
			timeout = time.Duration(num.RealValue() * float64(time.Second))
		} else {
			slip.TypePanic(s, depth, ":timeout", v, "real")
		}
	}
	if 0 < timeout {
		ctx, cf := context.WithTimeout(context.Background(), timeout)
		defer cf()
		err = self.Any.(jetstream.Msg).DoubleAck(ctx)
	} else {
		err = self.Any.(jetstream.Msg).Ack()
	}
	if err != nil {
		panic(err)
	}
	return nil
}

func (caller msgAckCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":ack",
		Text: `Ack the message which tells the server the message was processed successfully and
the next message can be made available. If a timeout is provided then the call
waits for an ack reply from the server.`,
		Args: []*slip.DocArg{
			{Name: "&key"},
			{
				Name: ":timeout",
				Type: "real",
				Text: `The timeout in seconds.`,
			},
		},
	}
}

type msgNakCaller struct{}

func (caller msgNakCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":nak", len(args), 0, 2)
	var (
		delay time.Duration
		err   error
	)
	if v, has := slip.GetArgsKeyValue(args, slip.Symbol(":delay")); has {
		if num, ok := v.(slip.Real); ok {
			delay = time.Duration(num.RealValue() * float64(time.Second))
		} else {
			slip.TypePanic(s, depth, ":delay", v, "real")
		}
	}
	if 0 < delay {
		err = self.Any.(jetstream.Msg).NakWithDelay(delay)
	} else {
		err = self.Any.(jetstream.Msg).Nak()
	}
	if err != nil {
		panic(err)
	}
	return nil
}

func (caller msgNakCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":nak",
		Text: `Nak negatively acknowledges a message. This tells the server to redeliver the
message immediately or with an optional deley.`,
		Args: []*slip.DocArg{
			{Name: "&key"},
			{
				Name: ":delay",
				Type: "real",
				Text: `The delay in seconds before redelivering.`,
			},
		},
	}
}

type msgInProgressCaller struct{}

func (caller msgInProgressCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	if err := self.Any.(jetstream.Msg).InProgress(); err != nil {
		panic(err)
	}
	return nil
}

func (caller msgInProgressCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":in-progress",
		Text: `Tells the server that this message is being worked on. It resets the
redelivery timer on the server.`,
	}
}

type msgTermCaller struct{}

func (caller msgTermCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	slip.CheckMethodArgCount(self, ":term", len(args), 0, 1)
	var err error
	if 0 < len(args) {
		reason := getStrArg(s, args[0], "reason", depth)
		err = self.Any.(jetstream.Msg).TermWithReason(reason)
	} else {
		err = self.Any.(jetstream.Msg).Term()
	}
	if err != nil {
		panic(err)
	}
	return nil
}

func (caller msgTermCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":term",
		Text: `Term tells the server to not redeliver this message. If _reason_ is provided
it is added to the server logs.`,
		Args: []*slip.DocArg{
			{Name: "&optional"},
			{
				Name: "reason",
				Type: "string",
				Text: `A reason for the term to add to the server logs.`,
			},
		},
	}
}

func getStrArg(s *slip.Scope, arg slip.Object, use string, depth int) string {
	ss, ok := arg.(slip.String)
	if !ok {
		slip.TypePanic(s, depth, use, arg, "string")
	}
	return string(ss)
}

func assocToHeader(s *slip.Scope, value slip.Object, field string, depth int) nats.Header {
	alist, ok := value.(slip.List)
	if !ok {
		slip.TypePanic(s, depth, field, value, "assoc")
	}
	header := nats.Header{}
	for _, element := range alist {
		elist, ok2 := element.(slip.List)
		if !ok2 || len(elist) < 2 {
			slip.TypePanic(s, depth, "assoc element", element, "list")
		}
		var (
			key    slip.String
			values []string
		)
		if key, ok = elist[0].(slip.String); !ok {
			slip.TypePanic(s, depth, "header key", elist[0], "string")
		}
		for _, v := range elist[1:] {
			var ss slip.String
			if ss, ok = v.(slip.String); !ok {
				slip.TypePanic(s, depth, "header value", v, "string")
			}
			values = append(values, string(ss))
		}
		header[string(key)] = values
	}
	return header
}
