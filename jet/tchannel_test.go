// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"testing"

	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/sliptest"
)

func TestTChannel(t *testing.T) {
	tc := jet.TChannel(make(chan struct{}, 3))
	(&sliptest.Object{
		Target:    tc,
		String:    "#<t-channel 3>",
		Simple:    "#<t-channel 3>",
		Hierarchy: "t-channel.channel.t",
		Equals: []*sliptest.EqTest{
			{Other: tc, Expect: true},
			{Other: slip.Fixnum(5), Expect: false},
		},
		Eval: tc,
	}).Test(t)
	tt.Equal(t, 0, tc.Length())
}
