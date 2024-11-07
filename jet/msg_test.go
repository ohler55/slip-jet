// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/sliptest"
)

func TestMsgDocs(t *testing.T) {
	scope := slip.NewScope()
	var out strings.Builder
	scope.Let(slip.Symbol("out"), &slip.OutputStream{Writer: &out})

	for _, method := range []string{
		":ack",
		":consumer",
		":consumer-sequence",
		":data",
		":domain",
		":headers",
		":in-progress",
		":init",
		":nak",
		":number-delivered",
		":number-pending",
		":stream",
		":stream-sequence",
		":subject",
		":term",
		":timestamp",
	} {
		_ = slip.ReadString(fmt.Sprintf(`(describe-method 'jet-msg %s out)`, method)).Eval(scope, nil)
		tt.Equal(t, true, strings.Contains(out.String(), method))
		out.Reset()
	}
}

func TestMsgConsumerSequence(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"),
		jet.MakeMsg(&jet.PubMsg{
			Meta: &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Consumer: 3}},
		}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :consumer-sequence)`,
		Expect: `3`,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :consumer-sequence)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestMsgStreamSequence(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"),
		jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: 3}}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :stream-sequence)`,
		Expect: `3`,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :stream-sequence)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestMsgNumberDelivered(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"), jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{NumDelivered: 7}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :number-delivered)`,
		Expect: `7`,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :number-delivered)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestMsgNumberPending(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"), jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{NumPending: 7}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :number-pending)`,
		Expect: `7`,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :number-pending)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestMsgTimestamp(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"),
		jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{
			Timestamp: time.Date(2024, time.November, 11, 13, 37, 1, 0, time.UTC),
		}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :timestamp)`,
		Expect: `@2024-11-11T13:37:01Z`,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :timestamp)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestMsgStream(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"), jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{Stream: "river"}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :stream)`,
		Expect: `"river"`,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :stream)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestMsgConsumer(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"), jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{Consumer: "drinker"}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :consumer)`,
		Expect: `"drinker"`,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :consumer)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestMsgDomain(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"), jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{Domain: "domino"}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :domain)`,
		Expect: `"domino"`,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :domain)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestMsgSubject(t *testing.T) {
	(&sliptest.Function{
		Source: `(send (make-instance 'jet-msg :subject "sub.ject") :subject)`,
		Expect: `"sub.ject"`,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(make-instance 'jet-msg :subject t)`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestMsgReply(t *testing.T) {
	(&sliptest.Function{
		Source: `(send (make-instance 'jet-msg :reply "re.ply") :reply)`,
		Expect: `"re.ply"`,
	}).Test(t)
}

func TestMsgHeaders(t *testing.T) {
	(&sliptest.Function{
		Source: `(send (make-instance 'jet-msg :headers '(("head" "str1" "str2"))) :headers)`,
		Expect: `(("head" "str1" "str2"))`,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(make-instance 'jet-msg :headers t)`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(make-instance 'jet-msg :headers '(t))`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(make-instance 'jet-msg :headers '((t "x")))`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(make-instance 'jet-msg :headers '(("a" t)))`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestMsgData(t *testing.T) {
	(&sliptest.Function{
		Source: `(send (make-instance 'jet-msg :data "abc") :data)`,
		Array:  true,
		Expect: "#(97 98 99)",
	}).Test(t)
	(&sliptest.Function{
		Source: `(send (make-instance 'jet-msg :data (coerce "abc" 'octets)) :data)`,
		Array:  true,
		Expect: "#(97 98 99)",
	}).Test(t)
	(&sliptest.Function{
		Source: `(send (make-instance 'jet-msg :data (make-instance 'bag-flavor :parse "{a:1}")) :data)`,
		Array:  true,
		Expect: "#(123 34 97 34 58 49 125)",
	}).Test(t)
	(&sliptest.Function{
		Source:    `(make-instance 'jet-msg :data (make-instance 'vanilla-flavor))`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(make-instance 'jet-msg :data t)`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestMsgAck(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"), jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :ack)`,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :ack :timeout 0.1)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :ack :timeout t)`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestMsgNak(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"), jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :nak)`,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :nak)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :nak :delay 0.1)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :nak :delay t)`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestMsgInProgress(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"), jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :in-progress)`,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :in-progress)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestMsgTerm(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"), jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :term)`,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :term)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :term "because")`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
	(&sliptest.Function{
		Source:    `(send (make-instance 'jet-msg) :term t)`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}
