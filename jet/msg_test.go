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
}

func TestMsgMsgNumberDelivered(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"), jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{NumDelivered: 7}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :number-delivered)`,
		Expect: `7`,
	}).Test(t)
}

func TestMsgMsgNumberPending(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"), jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{NumPending: 7}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :number-pending)`,
		Expect: `7`,
	}).Test(t)
}

func TestMsgMsgTimestamp(t *testing.T) {
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
}

func TestMsgMsgStream(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"), jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{Stream: "river"}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :stream)`,
		Expect: `"river"`,
	}).Test(t)
}

func TestMsgMsgConsumer(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"), jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{Consumer: "drinker"}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :consumer)`,
		Expect: `"drinker"`,
	}).Test(t)
}

func TestMsgMsgDomain(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("msg"), jet.MakeMsg(&jet.PubMsg{Meta: &jetstream.MsgMetadata{Domain: "domino"}}))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send msg :domain)`,
		Expect: `"domino"`,
	}).Test(t)
}
