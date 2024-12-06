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

func TestStreamGetMsgOptions(t *testing.T) {
	var stream mockStream
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send js :get-msg 123 :subject "test.sub" :timeout 0.1)`,
		Expect: "/#<jet-msg [0-9a-f]+>/",
	}).Test(t)
	tt.Equal(t, "GetMsg 123 called with timeout\n", string(stream.log))
}

func TestStreamGetMsgBadSequence(t *testing.T) {
	var stream mockStream
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send js :get-msg t)`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestStreamGetMsgError(t *testing.T) {
	var stream mockStream
	stream.err = fmt.Errorf("dummy error")
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send js :get-msg 123)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
