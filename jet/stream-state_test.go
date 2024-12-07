// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamState(t *testing.T) {
	var jss jetstream.StreamState
	sampleStreamState(&jss)
	state := jet.MakeStreamState(&jss)
	scope := slip.NewScope()
	scope.Let(slip.Symbol("state"), state)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send state :msgs)`,
		Expect: "100",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send state :bytes)`,
		Expect: "6000",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send state :first-seq)`,
		Expect: "3",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send state :first-time)`,
		Expect: "@2024-12-03T19:33:22.000000123Z",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send state :last-seq)`,
		Expect: "37",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send state :last-time)`,
		Expect: "@2024-12-03T20:33:22.000000123Z",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send state :consumers)`,
		Expect: "2",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send state :deleted)`,
		Expect: "(5 6 7)",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send state :number-deleted)`,
		Expect: "3",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send state :number-subjects)`,
		Expect: "1",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send state :subjects)`,
		Expect: `("test.sub" 7)`,
	}).Test(t)
}

func sampleStreamState(jss *jetstream.StreamState) {
	first := time.Date(2024, time.December, 3, 19, 33, 22, 123, time.UTC)
	last := time.Date(2024, time.December, 3, 20, 33, 22, 123, time.UTC)
	jss.Msgs = 100
	jss.Bytes = 6000
	jss.FirstSeq = 3
	jss.FirstTime = first
	jss.LastSeq = 37
	jss.LastTime = last
	jss.Consumers = 2
	jss.Deleted = []uint64{5, 6, 7}
	jss.NumDeleted = 3
	jss.NumSubjects = 1
	jss.Subjects = map[string]uint64{"test.sub": 7}
}
