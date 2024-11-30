// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestPublishAsyncString(t *testing.T) {
	streamName := "publish-async-test"
	js, _ := createStream(t, streamName, "test.async.>")
	defer func() { _ = js.DeleteStream(context.Background(), streamName) }()

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (future (send js :publish-async "hello" "test.async.pub" :stall-wait 0.5))
                                    (ack (send future :result)))
                              (send js :close)
                              (list (send ack :stream-name) (send ack :sequence-number)))`, natsURL),
		Expect: `("publish-async-test" 1)`,
	}).Test(t)
}

func TestPublishAsyncOctets(t *testing.T) {
	streamName := "publish-async-test"
	js, _ := createStream(t, streamName, "test.async.>")
	defer func() { _ = js.DeleteStream(context.Background(), streamName) }()

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (future (jet-publish-async js (coerce "hello" 'octets) "test.async.pub"))
                                    (ack (channel-pop future))) ;; verify it works with channel-pop
                              (send js :close)
                              (list (send ack :stream-name) (send ack :sequence-number)))`, natsURL),
		Expect: `("publish-async-test" 1)`,
	}).Test(t)
}

func TestPublishAsyncMsg(t *testing.T) {
	streamName := "publish-async-test"
	js, _ := createStream(t, streamName, "test.async.>")
	defer func() { _ = js.DeleteStream(context.Background(), streamName) }()

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (m (make-instance 'jet-msg :data "hello" :subject "test.async.pub"))
                                    (future (send js :publish-async m))
                                    (ack (send future :result)))
                              (send js :close)
                              (list (send ack :stream-name) (send ack :sequence-number)))`, natsURL),
		Expect: `("publish-async-test" 1)`,
	}).Test(t)
}

func TestPublishAsyncBadPayload(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), nil)
	defer func() {
		_ = slip.ReadString("(send js :close)").Eval(scope, nil)
	}()
	(&sliptest.Function{
		Scope: scope,
		Source: fmt.Sprintf(`(send (setq js (jet-connect :url %q :user "u1" :password "password")) :publish-async t)`,
			natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
	(&sliptest.Function{
		Scope: scope,
		Source: fmt.Sprintf(`(send (setq js (jet-connect :url %q :user "u1" :password "password"))
                                   :publish-async (make-instance 'vanilla-flavor))`,
			natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestPublishAsyncFail(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), nil)
	defer func() {
		_ = slip.ReadString("(send js :close)").Eval(scope, nil)
	}()
	(&sliptest.Function{
		Scope: scope,
		Source: fmt.Sprintf(`(send
                              (setq js (jet-connect :url %q :user "u1" :password "password"))
                              :publish-async "x" :subject "bad")`,
			natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
