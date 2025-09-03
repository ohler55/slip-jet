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

func TestConsumerFetch(t *testing.T) {
	var mc mockConsumer
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	sampleConsumerInfo(&mc.info, tm)
	mb := mockMessageBatch{msgs: []jetstream.Msg{}}
	mc.mb = &mb

	scope := slip.NewScope()
	scope.Let(slip.Symbol("mc"), jet.MakeConsumer(&mc))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(channel-pop (send (send mc :fetch 2 :max-wait 0) :messages))`,
		Expect: "nil",
	}).Test(t)

	mb.msgs = []jetstream.Msg{
		&jet.PubMsg{Body: []byte("hello"), Subj: "test.greeting"},
		&jet.PubMsg{Body: []byte("goodbye"), Subj: "test.greeting"},
	}
	mb.err = fmt.Errorf("dummy")
	(&sliptest.Function{
		Scope: scope,
		Source: `(let* ((mb (send mc :fetch 3 :max-wait 2.0 :heartbeat 1.5))
                        (msg-chan (send mb :messages))
                        (m1 (channel-pop msg-chan))
                        (m2 (channel-pop msg-chan))
                        (m3 (channel-pop msg-chan))
                        (err (send mb :error)))
                  (list (when m1 (coerce (send m1 :data) 'string))
                        (when m2 (coerce (send m2 :data) 'string))
                        (when m3 (coerce (send m3 :data) 'string))
                        (when err (slot-value err 'message))))`,
		Expect: `("hello" "goodbye" nil "dummy")`,
	}).Test(t)

	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send mc :fetch t)`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)

	mc.err = fmt.Errorf("dummy")
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send mc :fetch 1)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestConsumerFetchBytes(t *testing.T) {
	var mc mockConsumer
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	sampleConsumerInfo(&mc.info, tm)
	mb := mockMessageBatch{msgs: []jetstream.Msg{}}
	mc.mb = &mb

	scope := slip.NewScope()
	scope.Let(slip.Symbol("mc"), jet.MakeConsumer(&mc))
	mb.msgs = []jetstream.Msg{
		&jet.PubMsg{Body: []byte("hello"), Subj: "test.greeting"},
		&jet.PubMsg{Body: []byte("goodbye"), Subj: "test.greeting"},
	}
	mb.err = fmt.Errorf("dummy")
	(&sliptest.Function{
		Scope: scope,
		Source: `(let* ((mb (send mc :fetch-bytes 3000 :max-wait 2.0 :heartbeat 1.5))
                        (msg-chan (send mb :messages))
                        (m1 (channel-pop msg-chan))
                        (m2 (channel-pop msg-chan))
                        (m3 (channel-pop msg-chan))
                        (err (send mb :error)))
                  (list (when m1 (coerce (send m1 :data) 'string))
                        (when m2 (coerce (send m2 :data) 'string))
                        (when m3 (coerce (send m3 :data) 'string))
                        (when err (slot-value err 'message))))`,
		Expect: `("hello" "goodbye" nil "dummy")`,
	}).Test(t)

	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send mc :fetch-bytes t)`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)

	mc.err = fmt.Errorf("dummy")
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send mc :fetch-bytes 1)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestConsumerMessages(t *testing.T) {
	var mc mockConsumer
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	sampleConsumerInfo(&mc.info, tm)
	mmc := mockMessageContext{msg: &jet.PubMsg{Body: []byte("hello"), Subj: "test.greeting"}}
	mc.mc = &mmc

	scope := slip.NewScope()
	scope.Let(slip.Symbol("mc"), jet.MakeConsumer(&mc))
	(&sliptest.Function{
		Scope: scope,
		Source: `(let* ((mmc (send mc :messages :error-on-missing-heartbeat t :pull-max-messages 100)))
                  (coerce (send (send mmc :next) :data) 'string))`,
		Expect: `"hello"`,
	}).Test(t)

	mc.err = fmt.Errorf("dummy")
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send mc :messages)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestConsumerNext(t *testing.T) {
	var mc mockConsumer
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	sampleConsumerInfo(&mc.info, tm)
	mc.msg = &jet.PubMsg{Body: []byte("hello"), Subj: "test.greeting"}

	scope := slip.NewScope()
	scope.Let(slip.Symbol("mc"), jet.MakeConsumer(&mc))
	(&sliptest.Function{
		Scope:  scope,
		Source: `(coerce (send (send mc :next :max-wait 1 :heartbeat 2) :data) 'string)`,
		Expect: `"hello"`,
	}).Test(t)

	mc.err = fmt.Errorf("dummy")
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send mc :next)`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestConsumerConsume(t *testing.T) {
	var mc mockConsumer
	tm := time.Date(2024, time.December, 9, 19, 00, 2, 123, time.UTC)
	sampleConsumerInfo(&mc.info, tm)
	ccc := make(chan struct{}, 10)
	mc.cc.closed = ccc
	ccc <- struct{}{}
	close(ccc)

	mc.cc.msgs = []jetstream.Msg{
		&jet.PubMsg{Body: []byte("hello"), Subj: "test.greeting"},
	}

	scope := slip.NewScope()
	scope.Let(slip.Symbol("mc"), jet.MakeConsumer(&mc))
	(&sliptest.Function{
		Scope: scope,
		Source: `(let* (msgs
                        (cc (send mc :consume (lambda (m) (addf msgs (coerce (send m :data) 'string)))
                                     :stop-after 3
                                     :pull-expiry 10
                                     :pull-max-bytes 4000
                                     :pull-max-messages 4
                                     :pull-heartbeat 2
                                     :pull-threshold-bytes 4000
                                     :pull-threshold-messages 4
                                     :error-handler (lambda (c err) (addf msgs err)))))
                  (send cc :drain)
                  (send cc :stop)
                  (channel-pop (send cc :closed))
                  msgs)`,
		Expect: `("hello")`,
	}).Test(t)
	tt.Equal(t, "Drain()\nStop()\n", string(mc.cc.log))

	mc.err = fmt.Errorf("dummy")
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send mc :consume (lambda (m) nil))`,
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
