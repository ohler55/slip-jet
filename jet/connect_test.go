// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip-jet/jet"
	"github.com/ohler55/slip/pkg/flavors"
	"github.com/ohler55/slip/sliptest"
)

func TestClientConnectPassword(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let ((js (jet-connect :url %q :user "u1" :password "password")))
                              (send js :close)
                              js)`, natsURL),
		Expect: "/#<jet-client [0-9a-f]+>/",
	}).Test(t)
}

func TestClientConnectAllowReconnect(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q
                                                     :user "u1" :password "password"
                                                     :allow-reconnect t))
                                    (value (get (send js :options) :allow-reconnect)))
                              (send js :close)
                              value)`, natsURL),
		Expect: "t",
	}).Test(t)
}

func TestClientConnectAsyncErrorCallback(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), nil)
	scope.Let(slip.Symbol("out"), nil)
	defer func() {
		_ = slip.ReadString("(send js :close)").Eval(scope, nil)
	}()
	(&sliptest.Function{
		Scope: scope,
		Source: fmt.Sprintf(`(setq js
                                   (jet-connect :url %q
                                                :user "u1" :password "password"
                                                :async-error-callback (lambda (c sub err) (setq out 'error))))`,
			natsURL),
		Expect: "/#<jet-client [0-9a-f]+>/",
	}).Test(t)
	inst, ok := scope.Get("js").(*flavors.Instance)
	tt.Equal(t, true, ok)
	nc := inst.Any.(*jet.Client).NatsConn()
	tt.NotNil(t, nc.Opts.AsyncErrorCB)
	nc.Opts.AsyncErrorCB(nc, nil, fmt.Errorf("dummy"))
	tt.Equal(t, slip.Symbol("error"), scope.Get("out"))
}

func TestClientConnectClosedHandler(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((cc (make-channel 2))
                                    (js (jet-connect :url %q
                                                     :user "u1" :password "password"
                                                     :closed-callback (lambda (c) (channel-push cc 'closed))))
                                    popped)
                              (send js :close)
                              (setq popped (channel-pop cc))
                              (channel-close cc)
                              popped)`, natsURL),
		Expect: "closed",
	}).Test(t)
}

func TestClientConnectCompression(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :compression t))
                                    (comp (get (send js :options) :compression)))
                              (send js :close)
                              comp)`, natsURL),
		Expect: "t",
	}).Test(t)
}

func TestClientConnectConnectedCallback(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((cc (make-channel 2))
                                    (js (jet-connect :url %q
                                                     :user "u1" :password "password"
                                                     :connected-callback (lambda (c) (channel-push cc 'connected))))
                                    popped)
                              (send js :close)
                              (setq popped (channel-pop cc))
                              (channel-close cc)
                              popped)`, natsURL),
		Expect: "connected",
	}).Test(t)
}
