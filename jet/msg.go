// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet

import (
	"context"
	"fmt"
	"time"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"

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
				slip.Symbol(":documentation"),
				slip.String(`
Includes methods for accessing information in a message. Methods for ack and nak are also included.

`),
			},
		},
		&Pkg,
	)
	msgFlavor.GoMakeOnly = true

	msgFlavor.DefMethod(":consumer-sequence", "", msgConsumerSequenceCaller{})
	flavors.FlosFun("jet-msg-consumer-sequence", ":consumer-sequence", msgConsumerSequenceCaller{}.Docs(), &Pkg)

	msgFlavor.DefMethod(":stream-sequence", "", msgStreamSequenceCaller{})
	flavors.FlosFun("jet-msg-stream-sequence", ":stream-sequence", msgStreamSequenceCaller{}.Docs(), &Pkg)

	msgFlavor.DefMethod(":number-delivered", "", msgNumberDeliveredCaller{})
	flavors.FlosFun("jet-msg-number-delivered", ":number-delivered", msgNumberDeliveredCaller{}.Docs(), &Pkg)

	msgFlavor.DefMethod(":number-pending", "", msgNumberPendingCaller{})
	flavors.FlosFun("jet-msg-number-pending", ":number-pending", msgNumberPendingCaller{}.Docs(), &Pkg)

	msgFlavor.DefMethod(":timestamp", "", msgTimestampCaller{})
	flavors.FlosFun("jet-msg-timestamp", ":number-pending", msgTimestampCaller{}.Docs(), &Pkg)

	msgFlavor.DefMethod(":stream", "", msgStreamCaller{})
	flavors.FlosFun("jet-msg-stream", ":stream", msgStreamCaller{}.Docs(), &Pkg)

	msgFlavor.DefMethod(":consumer", "", msgConsumerCaller{})
	flavors.FlosFun("jet-msg-consumer", ":consumer", msgConsumerCaller{}.Docs(), &Pkg)

	msgFlavor.DefMethod(":domain", "", msgDomainCaller{})
	flavors.FlosFun("jet-msg-domain", ":domain", msgDomainCaller{}.Docs(), &Pkg)

	msgFlavor.DefMethod(":data", "", msgDataCaller{})
	flavors.FlosFun("jet-msg-data", ":data", msgDataCaller{}.Docs(), &Pkg)

	msgFlavor.DefMethod(":headers", "", msgHeadersCaller{})
	flavors.FlosFun("jet-msg-headers", ":headers", msgHeadersCaller{}.Docs(), &Pkg)

	msgFlavor.DefMethod(":subject", "", msgSubjectCaller{})
	flavors.FlosFun("jet-msg-subject", ":subject", msgSubjectCaller{}.Docs(), &Pkg)

	msgFlavor.DefMethod(":ack", "", msgAckCaller{})
	flavors.FlosFun("jet-msg-ack", ":ack", msgAckCaller{}.Docs(), &Pkg)

	msgFlavor.DefMethod(":nak", "", msgNakCaller{})
	flavors.FlosFun("jet-msg-nak", ":nak", msgNakCaller{}.Docs(), &Pkg)

	msgFlavor.DefMethod(":in-progress", "", msgInProgressCaller{})
	flavors.FlosFun("jet-msg-in-progress", ":in-progress", msgInProgressCaller{}.Docs(), &Pkg)

	msgFlavor.DefMethod(":term", "", msgTermCaller{})
	flavors.FlosFun("jet-msg-term", ":term", msgTermCaller{}.Docs(), &Pkg)
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

type msgConsumerSequenceCaller struct{}

func (caller msgConsumerSequenceCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	meta, err := self.Any.(jetstream.Msg).Metadata()
	if err != nil {
		panic(err)
	}
	return slip.Fixnum(meta.Sequence.Consumer)
}

func (caller msgConsumerSequenceCaller) Docs() string {
	return `__:consumer-sequence__ => _fixnum_


Returns the consumer sequence for the message.
`
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

func (caller msgStreamSequenceCaller) Docs() string {
	return `__:stream-sequence__ => _fixnum_


Returns the stream sequence for the message.
`
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

func (caller msgNumberDeliveredCaller) Docs() string {
	return `__:number-delivered__ => _fixnum_


Returns the number of times the associated message was delivered to the consumer.
`
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

func (caller msgNumberPendingCaller) Docs() string {
	return `__:number-pending__ => _fixnum_


Returns the number of messages pending that match the consumer's filter.
`
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

func (caller msgTimestampCaller) Docs() string {
	return `__:timestamp__ => _time_


Returns the time the message was originally stored on a stream.
`
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

func (caller msgStreamCaller) Docs() string {
	return `__:stream__ => _string_


Returns the stream name this message is stored on.
`
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

func (caller msgConsumerCaller) Docs() string {
	return `__:consumer__ => _string_


Returns the consumer name this message was delivered to.
`
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

func (caller msgDomainCaller) Docs() string {
	return `__:domain__ => _string_


Returns the domain name this message was received on.
`
}

type msgDataCaller struct{}

func (caller msgDataCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)

	return slip.Octets(self.Any.(jetstream.Msg).Data())
}

func (caller msgDataCaller) Docs() string {
	return `__:data__ => _octets_


Returns the data for a message.
`
}

type msgHeadersCaller struct{}

func (caller msgHeadersCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)

	headers := self.Any.(jetstream.Msg).Headers()
	fmt.Printf("*** %v\n", headers)

	return nil // TBD
}

func (caller msgHeadersCaller) Docs() string {
	return `__:headers__ => _TBD_


Returns the headers for a message.
`
}

type msgSubjectCaller struct{}

func (caller msgSubjectCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)

	return slip.String(self.Any.(jetstream.Msg).Subject())
}

func (caller msgSubjectCaller) Docs() string {
	return `__:subject__ => _string_


Returns the subject the message was published and received on.
`
}

type msgAckCaller struct{}

func (caller msgAckCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.PanicMethodArgCount(self, ":ack", len(args), 0, 1)
	var (
		timeout time.Duration
		err     error
	)
	if v, has := slip.GetArgsKeyValue(args[1:], slip.Symbol(":timeout")); has {
		if num, ok := v.(slip.Real); ok {
			timeout = time.Duration(num.RealValue() * float64(time.Second))
		} else {
			slip.PanicType(":timeout", v, "real")
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

func (caller msgAckCaller) Docs() string {
	return `__:ack__ &key _timeout_
   _:timeout_ [real] the
timeout in seconds.


Ack the message which tells the server the message was processed successfully and
the next message can be made available. If a timeout is provided then the call
waits for an ack reply from the server.
`
}

type msgNakCaller struct{}

func (caller msgNakCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.PanicMethodArgCount(self, ":nak", len(args), 0, 1)
	var (
		delay time.Duration
		err   error
	)
	if v, has := slip.GetArgsKeyValue(args[1:], slip.Symbol(":delay")); has {
		if num, ok := v.(slip.Real); ok {
			delay = time.Duration(num.RealValue() * float64(time.Second))
		} else {
			slip.PanicType(":delay", v, "real")
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

func (caller msgNakCaller) Docs() string {
	return `__:nak__ &key _delay_
   _:delay_ [real] the delay in seconds before redelivering.


Nak negatively acknowledges a message. This tells the server to redeliver the
message immediately or with an optional deley.
`
}

type msgInProgressCaller struct{}

func (caller msgInProgressCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)

	if err := self.Any.(jetstream.Msg).InProgress(); err != nil {
		panic(err)
	}
	return nil
}

func (caller msgInProgressCaller) Docs() string {
	return `__:in-progress__


Tells the server that this message is being worked on. It resets the
redelivery timer on the server.
`
}

type msgTermCaller struct{}

func (caller msgTermCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	flavors.PanicMethodArgCount(self, ":term", len(args), 0, 1)
	var (
		err    error
		reason string
	)
	if 0 < len(reason) {
		err = self.Any.(jetstream.Msg).TermWithReason(reason)
	} else {
		err = self.Any.(jetstream.Msg).Term()
	}
	if err != nil {
		panic(err)
	}
	return nil
}

func (caller msgTermCaller) Docs() string {
	return `__:term__ &optional _reason_
   _reason_ [string] a reason for the term to add to the server logs.

Term tells the server to not redeliver this message. If _reason_ is provided
it is added to the server logs.
`
}
