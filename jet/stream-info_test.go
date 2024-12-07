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

func TestStreamInfoCached(t *testing.T) {
	var stream mockStream
	sampleStreamConfig(&stream.info.Config)
	sampleStreamState(&stream.info.State)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send js :info :cached t)`,
		Validate: func(t *testing.T, v slip.Object) {
			values, ok := v.(slip.Values)
			tt.Equal(t, true, ok)
			state := values[0].(slip.List)
			cfg := values[1].(slip.List)
			// The contents of both state and cfg are tested in other tests so
			// just check of few to make sure they are property lists for
			// each.
			checkPlistValue(t, ":bytes", state, slip.Fixnum(6000))
			checkPlistValue(t, ":name", cfg, slip.String("river"))
		},
	}).Test(t)
}

func TestStreamInfoServer(t *testing.T) {
	var stream mockStream
	sampleStreamConfig(&stream.info.Config)
	sampleStreamState(&stream.info.State)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send js :info :timeout 0.1 :deleted t)`,
		Validate: func(t *testing.T, v slip.Object) {
			values, ok := v.(slip.Values)
			tt.Equal(t, true, ok)
			state := values[0].(slip.List)
			cfg := values[1].(slip.List)
			// The contents of both state and cfg are tested in other tests so
			// just check of few to make sure they are property lists for
			// each.
			checkPlistValue(t, ":bytes", state, slip.Fixnum(6000))
			checkPlistValue(t, ":name", cfg, slip.String("river"))
		},
	}).Test(t)
	stream.err = fmt.Errorf("dummy error")
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send js :info :timeout 0.1)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
