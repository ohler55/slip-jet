// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamState(t *testing.T) {
	var jss jetstream.StreamState
	sampleStreamState(&jss)
	plist := jet.StreamStatePropList(&jss)
	checkPlistValue(t, ":msgs", plist, slip.Fixnum(100))
	checkPlistValue(t, ":bytes", plist, slip.Fixnum(6000))
	checkPlistValue(t, ":first-seq", plist, slip.Fixnum(3))
	checkPlistValue(t, ":first-time", plist, slip.Time(jss.FirstTime))
	checkPlistValue(t, ":last-seq", plist, slip.Fixnum(37))
	checkPlistValue(t, ":last-time", plist, slip.Time(jss.LastTime))
	checkPlistValue(t, ":consumers", plist, slip.Fixnum(2))
	checkPlistValue(t, ":deleted", plist, slip.List{slip.Fixnum(5), slip.Fixnum(6), slip.Fixnum(7)})
	checkPlistValue(t, ":number-deleted", plist, slip.Fixnum(3))
	checkPlistValue(t, ":number-subjects", plist, slip.Fixnum(1))
	checkPlistValue(t, ":subjects", plist, slip.List{slip.String("test.sub"), slip.Fixnum(7)})
}

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
