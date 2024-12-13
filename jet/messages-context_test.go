// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/sliptest"
)

func TestMessageContext(t *testing.T) {
	mmc := mockMessageContext{
		msg: &jet.PubMsg{Body: []byte("hello"), Subj: "test.greeting"},
	}
	mc := jet.MakeMessagesContext(&mmc)
	scope := slip.NewScope()
	scope.Let(slip.Symbol("mc"), mc)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(let ((m (send mc :next))) (send mc :drain) (send mc :stop) m)`,
		Expect: "/#<jet-msg [0-9a-f]+>/",
	}).Test(t)
	tt.Equal(t, `Drain()
Stop()
`, string(mmc.log))

	mmc.err = fmt.Errorf("dummy")
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send mc :next)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
