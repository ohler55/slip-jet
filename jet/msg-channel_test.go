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
