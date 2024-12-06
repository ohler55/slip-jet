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

func TestStreamPurgeKeep(t *testing.T) {
	var stream mockStream
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send js :purge :keep 123 :subject "test.sub")`,
		Expect: "nil",
	}).Test(t)
	tt.Equal(t, "Purge called with {keep: 123 sequence: 0 subject: test.sub}\n", string(stream.log))
}

func TestStreamPurgeSequence(t *testing.T) {
	var stream mockStream
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send js :purge :timeout 0.1 :sequence 123)`,
		Expect: "nil",
	}).Test(t)
	tt.Equal(t, `Purge called with {keep: 0 sequence: 123 subject: ""} with deadline
`, string(stream.log))
}

func TestStreamPurgeError(t *testing.T) {
	var stream mockStream
	stream.err = fmt.Errorf("some error")
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send js :purge)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestStreamPurgeBadKeep(t *testing.T) {
	var stream mockStream
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send js :purge :keep t)`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestStreamPurgeBadSequence(t *testing.T) {
	var stream mockStream
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send js :purge :sequence t)`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}
