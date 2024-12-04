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
	first := time.Date(2024, time.December, 3, 19, 33, 22, 123, time.UTC)
	last := time.Date(2024, time.December, 3, 20, 33, 22, 123, time.UTC)
	jss := jetstream.StreamState{
		Msgs:        100,
		Bytes:       6000,
		FirstSeq:    3,
		FirstTime:   first,
		LastSeq:     37,
		LastTime:    last,
		Consumers:   2,
		Deleted:     []uint64{5, 6, 7},
		NumDeleted:  3,
		NumSubjects: 1,
		Subjects:    map[string]uint64{"test.sub": 7},
	}
	plist := jet.StreamStatePropList(&jss)
	checkPlistValue(t, ":msgs", plist, slip.Fixnum(100))
	checkPlistValue(t, ":bytes", plist, slip.Fixnum(6000))
	checkPlistValue(t, ":first-seq", plist, slip.Fixnum(3))
	checkPlistValue(t, ":first-time", plist, slip.Time(first))
	checkPlistValue(t, ":last-seq", plist, slip.Fixnum(37))
	checkPlistValue(t, ":last-time", plist, slip.Time(last))
	checkPlistValue(t, ":consumers", plist, slip.Fixnum(2))
	checkPlistValue(t, ":deleted", plist, slip.List{slip.Fixnum(5), slip.Fixnum(6), slip.Fixnum(7)})
	checkPlistValue(t, ":number-deleted", plist, slip.Fixnum(3))
	checkPlistValue(t, ":number-subjects", plist, slip.Fixnum(1))
	checkPlistValue(t, ":subjects", plist, slip.List{slip.String("test.sub"), slip.Fixnum(7)})
}

func TestStreamInfo(t *testing.T) {
	stream := mockStream{
		// TBD
	}
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), jet.MakeStream(&stream))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send js :info :cached t)`,
		Validate: func(t *testing.T, v slip.Object) {
			values, ok := v.(slip.Values)
			tt.Equal(t, true, ok)
			fmt.Printf("*** values: %s\n", values)
		},
	}).Test(t)
}
