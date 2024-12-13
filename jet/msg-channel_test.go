// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/sliptest"
)

func TestMsgChannel(t *testing.T) {
	mc := jet.MsgChannel(make(chan jetstream.Msg, 3))
	(&sliptest.Object{
		Target:    mc,
		String:    "#<jet-msg-channel 3>",
		Simple:    "#<jet-msg-channel 3>",
		Hierarchy: "jet-msg-channel.channel.t",
		Equals: []*sliptest.EqTest{
			{Other: mc, Expect: true},
			{Other: slip.Fixnum(5), Expect: false},
		},
		Eval: mc,
	}).Test(t)
	tt.Equal(t, 0, mc.Length())
}

func TestMsgChannelRange(t *testing.T) {
	mc := make(chan jetstream.Msg, 3)
	mc <- &jet.PubMsg{Body: []byte("hello"), Subj: "test.greeting"}
	mc <- &jet.PubMsg{Body: []byte("goodbye"), Subj: "test.greeting"}
	close(mc)
	scope := slip.NewScope()
	scope.Let(slip.Symbol("tc"), jet.MsgChannel(mc))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(let (result) (range (lambda (m) (addf result (coerce (send m :data) 'string))) tc) result)`,
		Expect: `("hello" "goodbye")`,
	}).Test(t)
}
