// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamSubjects(t *testing.T) {
	var stream mockStream
	sampleStreamConfig(&stream.info.Config)
	sampleStreamState(&stream.info.State)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send js :subjects)`,
		Expect: `("test.one" "test.two")`,
	}).Test(t)
}
