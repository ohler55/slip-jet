// Copyright (c) 2025, Peter Ohler, All rights reserved.

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

func TestPushConsumerInfoCached(t *testing.T) {
	var mc mockPushConsumer
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	sampleConsumerInfo(&mc.info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("mc"), jet.MakePushConsumer(&mc))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send mc :info :cached t)`,
		Expect: "/#<jet-consumer-info [0-9a-f]+>/",
	}).Test(t)
}

func TestPushConsumerInfoNotCached(t *testing.T) {
	var mc mockPushConsumer
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	sampleConsumerInfo(&mc.info, tm)

	scope := slip.NewScope()
	scope.Let(slip.Symbol("mc"), jet.MakePushConsumer(&mc))
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

func TestPushConsumerConsume(t *testing.T) {
	var mc mockPushConsumer
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	sampleConsumerInfo(&mc.info, tm)
	ccc := make(chan struct{}, 10)
	mc.cc.closed = ccc
	ccc <- struct{}{}
	close(ccc)

	mc.cc.msgs = []jetstream.Msg{
		&jet.PubMsg{Body: []byte("hello"), Subj: "test.greeting"},
	}
	mc.err = fmt.Errorf("dummy")

	scope := slip.NewScope()
	scope.Let(slip.Symbol("mc"), jet.MakePushConsumer(&mc))
	(&sliptest.Function{
		Scope: scope,
		Source: `(let* (msgs
                        (cc (send mc :consume (lambda (m) (addf msgs (coerce (send m :data) 'string)))
                                     :error-handler (lambda (c err) (addf msgs err)))))
                  msgs)`,
		Expect: `/\("hello" #<error [0-9a-f]+>\)/`,
	}).Test(t)

	mc.err = fmt.Errorf("dummy")
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send mc :consume (lambda (m) nil))`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
