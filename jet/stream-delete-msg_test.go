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

func TestStreamDeleteMsgSecure(t *testing.T) {
	var stream mockStream
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send js :delete-msg 123 :secure t :timeout 0.1)`,
		Expect: "nil",
	}).Test(t)
	tt.Equal(t, "SecureDeleteMsg 123 called with timeout\n", string(stream.log))
}

func TestStreamDeleteMsgUnsecure(t *testing.T) {
	var stream mockStream
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send js :delete-msg 123)`,
		Expect: "nil",
	}).Test(t)
	tt.Equal(t, "DeleteMsg 123 called\n", string(stream.log))
}

func TestStreamDeleteMsgBadSequence(t *testing.T) {
	var stream mockStream
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send js :delete-msg t)`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestStreamDeleteMsgError(t *testing.T) {
	var stream mockStream
	stream.err = fmt.Errorf("dummy error")
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send js :delete-msg 123)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
