// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/sliptest"
)

func TestConsumerInfoCached(t *testing.T) {
	var mc mockConsumer
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	sampleConsumerInfo(&mc.info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("mc"), jet.MakeConsumer(&mc))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send mc :info :cached t)`,
		Expect: "/#<jet-consumer-info [0-9a-f]+>/",
	}).Test(t)
}

func TestConsumerInfoNotCached(t *testing.T) {
	var mc mockConsumer
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	sampleConsumerInfo(&mc.info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("mc"), jet.MakeConsumer(&mc))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send mc :info :timeout 0.1)`,
		Expect: "/#<jet-consumer-info [0-9a-f]+>/",
	}).Test(t)
	mc.err = fmt.Errorf("dummy")
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send mc :info :timeout 0.1)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestConsumerFetchChannel(t *testing.T) {
	var mc mockConsumer
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	sampleConsumerInfo(&mc.info, tm)
	mc.mb = &mockMessageBatch{msgs: []jetstream.Msg{}}

	scope := slip.NewScope()
	scope.Let(slip.Symbol("mc"), jet.MakeConsumer(&mc))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(channel-pop (send (send mc :fetch 2 :max-wait 0) :messages))`,
		Expect: "nil",
	}).Test(t)

	// TBD test with multiple messages
}
