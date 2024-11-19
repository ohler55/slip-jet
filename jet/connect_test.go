// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"
	"time"

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

func TestClientConnectCustomReconnectDelayCallback(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), nil)
	defer func() {
		_ = slip.ReadString("(send js :close)").Eval(scope, nil)
	}()
	(&sliptest.Function{
		Scope: scope,
		Source: fmt.Sprintf(`(setq js
                                   (jet-connect :url %q
                                                :user "u1" :password "password"
                                                :custom-reconnect-delay-callback (lambda (n) (* n 2))))`,
			natsURL),
		Expect: "/#<jet-client [0-9a-f]+>/",
	}).Test(t)
	inst, ok := scope.Get("js").(*flavors.Instance)
	tt.Equal(t, true, ok)
	nc := inst.Any.(*jet.Client).NatsConn()
	tt.NotNil(t, nc.Opts.CustomReconnectDelayCB)
	result := nc.Opts.CustomReconnectDelayCB(3)
	tt.Equal(t, time.Duration(time.Second*6), result)
}

func TestClientConnectCustomReconnectDelayCallbackError(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), nil)
	(&sliptest.Function{
		Scope: scope,
		Source: fmt.Sprintf(`(setq js
                                   (jet-connect :url %q
                                                :user "u1" :password "password"
                                                :custom-reconnect-delay-callback (lambda (n) t)))`,
			natsURL),
		Expect: "/#<jet-client [0-9a-f]+>/",
	}).Test(t)
	inst, ok := scope.Get("js").(*flavors.Instance)
	tt.Equal(t, true, ok)
	nc := inst.Any.(*jet.Client).NatsConn()
	tt.NotNil(t, nc.Opts.CustomReconnectDelayCB)
	tt.Panic(t, func() { _ = nc.Opts.CustomReconnectDelayCB(3) })
}

func TestClientConnectDisconnectedCallback(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((cc (make-channel 2))
                                    (js (jet-connect :url %q
                                                     :user "u1" :password "password"
                                                     :disconnected-callback (lambda (c) (channel-push cc 'done))))
                                    popped)
                              (send js :close)
                              (setq popped (channel-pop cc))
                              (channel-close cc)
                              popped)`, natsURL),
		Expect: "done",
	}).Test(t)
}

func TestClientConnectCustomDisconnectedErrorCallback(t *testing.T) {
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
                                                :disconnected-error-callback (lambda (c err) (setq out 'ok))))`,
			natsURL),
		Expect: "/#<jet-client [0-9a-f]+>/",
	}).Test(t)
	inst, ok := scope.Get("js").(*flavors.Instance)
	tt.Equal(t, true, ok)
	nc := inst.Any.(*jet.Client).NatsConn()
	tt.NotNil(t, nc.Opts.DisconnectedErrCB)
	nc.Opts.DisconnectedErrCB(nc, fmt.Errorf("dummy"))
	tt.Equal(t, slip.Symbol("ok"), scope.Get("out"))
}

func TestClientConnectCustomDiscoveredServersCallback(t *testing.T) {
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
                                                :discovered-servers-callback (lambda (c) (setq out 'ok))))`,
			natsURL),
		Expect: "/#<jet-client [0-9a-f]+>/",
	}).Test(t)
	inst, ok := scope.Get("js").(*flavors.Instance)
	tt.Equal(t, true, ok)
	nc := inst.Any.(*jet.Client).NatsConn()
	tt.NotNil(t, nc.Opts.DiscoveredServersCB)
	nc.Opts.DiscoveredServersCB(nc)
	tt.Equal(t, slip.Symbol("ok"), scope.Get("out"))
}

func TestClientConnectDrainTimeout(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :drain-timeout 20))
                                    (dt (/ (get (send js :options) :drain-timeout) internal-time-units-per-second)))
                              (send js :close)
                              dt)`, natsURL),
		Expect: "20",
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :drain-timeout t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectFlusherTimeout(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :flusher-timeout 20))
                                    (dt (/ (get (send js :options) :flusher-timeout) internal-time-units-per-second)))
                              (send js :close)
                              dt)`, natsURL),
		Expect: "20",
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :flusher-timeout t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectIgnoreAuthErrorAbort(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :ignore-auth-error-abort 7))
                                    (val (get (send js :options) :ignore-auth-error-abort)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "t",
	}).Test(t)
}

func TestClientConnectInboxPrefix(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :inbox-prefix "pre"))
                                    (pre (get (send js :options) :inbox-prefix)))
                              (send js :close)
                              pre)`, natsURL),
		Expect: `"pre"`,
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :inbox-prefix t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectLameDuckModeHandler(t *testing.T) {
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
                                                :lame-duck-mode-handler (lambda (c) (setq out 'ok))))`,
			natsURL),
		Expect: "/#<jet-client [0-9a-f]+>/",
	}).Test(t)
	inst, ok := scope.Get("js").(*flavors.Instance)
	tt.Equal(t, true, ok)
	nc := inst.Any.(*jet.Client).NatsConn()
	tt.NotNil(t, nc.Opts.LameDuckModeHandler)
	nc.Opts.LameDuckModeHandler(nc)
	tt.Equal(t, slip.Symbol("ok"), scope.Get("out"))
}

func TestClientConnectMaxPingsOut(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :max-pings-out 3))
                                    (val (get (send js :options) :max-pings-out)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "3",
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :max-pings-out t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectMaxReconnect(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :max-reconnect 3))
                                    (val (get (send js :options) :max-reconnect)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "3",
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :max-reconnect t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectName(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :name "namae"))
                                    (val (get (send js :options) :name)))
                              (send js :close)
                              val)`, natsURL),
		Expect: `"namae"`,
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :name t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

// TBD needs a server that supports nkeys, maybe just add nkeys users
// func TestClientConnectNkey(t *testing.T) {
// 	(&sliptest.Function{
// 		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"
//                                                      :signature-callback (lambda (b) b)
//                                                      :nkey "key"))
//                                     (val (get (send js :options) :nkey)))
//                               (send js :close)
//                               val)`, natsURL),
// 		Expect: `"key"`,
// 	}).Test(t)
// 	(&sliptest.Function{
// 		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :nkey t)`, natsURL),
// 		PanicType: slip.TypeErrorSymbol,
// 	}).Test(t)
// }

func TestClientConnectNoCallbacksAfterClientClose(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"
                                                     :no-callbacks-after-client-close 7))
                                    (val (get (send js :options) :no-callbacks-after-client-close)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "t",
	}).Test(t)
}

func TestClientConnectNoEcho(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :no-echo 7))
                                    (val (get (send js :options) :no-echo)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "t",
	}).Test(t)
}

func TestClientConnectNoRandomize(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :no-randomize 7))
                                    (val (get (send js :options) :no-randomize)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "t",
	}).Test(t)
}

func TestClientConnectBadPassword(t *testing.T) {
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectPedantic(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :pedantic 7))
                                    (val (get (send js :options) :pedantic)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "t",
	}).Test(t)
}

func TestClientConnectPingInterval(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :ping-interval 3))
                                    (val (/ (get (send js :options) :ping-interval) internal-time-units-per-second)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "3",
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :ping-interval t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectProxyPath(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :proxy-path "prox"))
                                    (val (get (send js :options) :proxy-path)))
                              (send js :close)
                              val)`, natsURL),
		Expect: `"prox"`,
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :proxy-path t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectReconnectBufSize(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :reconnect-buf-size 3000))
                                    (val (get (send js :options) :reconnect-buf-size)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "3000",
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :reconnect-buf-size t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectReconnectJitter(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :reconnect-jitter 0.3))
                                    (val (/ (get (send js :options) :reconnect-jitter) internal-time-units-per-second)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "0.3",
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :reconnect-jitter t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectReconnectJitterTLS(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :reconnect-jitter-tls 0.3))
                                    (val (/ (get (send js :options) :reconnect-jitter-tls)
                                            internal-time-units-per-second)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "0.3",
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :reconnect-jitter-tls t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectReconnectWait(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :reconnect-wait 2))
                                    (val (/ (get (send js :options) :reconnect-wait) internal-time-units-per-second)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "2",
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :reconnect-wait t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectReconnectedCallback(t *testing.T) {
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
                                                :reconnected-callback (lambda (c) (setq out 'ok))))`,
			natsURL),
		Expect: "/#<jet-client [0-9a-f]+>/",
	}).Test(t)
	inst, ok := scope.Get("js").(*flavors.Instance)
	tt.Equal(t, true, ok)
	nc := inst.Any.(*jet.Client).NatsConn()
	tt.NotNil(t, nc.Opts.ReconnectedCB)
	nc.Opts.ReconnectedCB(nc)
	tt.Equal(t, slip.Symbol("ok"), scope.Get("out"))
}

func TestClientConnectRetryOnFailedConnect(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"
                                                     :retry-on-failed-connect 7))
                                    (val (get (send js :options) :retry-on-failed-connect)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "t",
	}).Test(t)
}

// TBD secure connection not available so change server config
// func TestClientConnectSecure(t *testing.T) {
// 	(&sliptest.Function{
// 		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :secure 7))
//                                     (val (get (send js :options) :secure)))
//                               (send js :close)
//                               val)`, natsURL),
// 		Expect: "t",
// 	}).Test(t)
// }

func TestClientConnectServers(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"
                                                     :servers '(%q)))
                                    (val (get (send js :options) :servers)))
                              (send js :close)
                              val)`, natsURL, natsURL),
		Expect: `/"nats:\/\/127.0.0.1:[0-9]+"/`,
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :servers '(t))`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :servers t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectSignatureCallbackOk(t *testing.T) {
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
                                                :signature-callback (lambda (b) (setq out 'ok) (coerce "x" 'octets))))`,
			natsURL),
		Expect: "/#<jet-client [0-9a-f]+>/",
	}).Test(t)
	inst, ok := scope.Get("js").(*flavors.Instance)
	tt.Equal(t, true, ok)
	nc := inst.Any.(*jet.Client).NatsConn()
	tt.NotNil(t, nc.Opts.SignatureCB)
	_, err := nc.Opts.SignatureCB([]byte{'x'})
	tt.Nil(t, err)
	tt.Equal(t, slip.Symbol("ok"), scope.Get("out"))
}

func TestClientConnectSignatureCallbackError(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), nil)
	defer func() {
		_ = slip.ReadString("(send js :close)").Eval(scope, nil)
	}()
	(&sliptest.Function{
		Scope: scope,
		Source: fmt.Sprintf(`(setq js
                                   (jet-connect :url %q
                                                :user "u1" :password "password"
                                                :signature-callback (lambda (b) (panic "error"))))`,
			natsURL),
		Expect: "/#<jet-client [0-9a-f]+>/",
	}).Test(t)
	inst, ok := scope.Get("js").(*flavors.Instance)
	tt.Equal(t, true, ok)
	nc := inst.Any.(*jet.Client).NatsConn()
	tt.NotNil(t, nc.Opts.SignatureCB)
	_, err := nc.Opts.SignatureCB([]byte{'x'})
	tt.NotNil(t, err)
}

func TestClientConnectSkipHostLookup(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"
                                                     :skip-host-lookup 7))
                                    (val (get (send js :options) :skip-host-lookup)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "t",
	}).Test(t)
}

func TestClientConnectSubChanLen(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :sub-chan-len 111111))
                                    (val (get (send js :options) :sub-chan-len)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "111111",
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :sub-chan-len t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectTimeout(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :timeout 2))
                                    (dt (/ (get (send js :options) :timeout) internal-time-units-per-second)))
                              (send js :close)
                              dt)`, natsURL),
		Expect: "2",
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :timeout t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

// TBD tls: first record does not look like a TLS handshake
// func TestClientConnectTLSHandshakeFirst(t *testing.T) {
// 	(&sliptest.Function{
// 		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"
//                                                      :tls-handshake-first 7))
//                                     (val (get (send js :options) :tls-handshake-first)))
//                               (send js :close)
//                               val)`, natsURL),
// 		Expect: "t",
// 	}).Test(t)
// }

func TestClientConnectToken(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password" :token "xyz"))
                                    (val (get (send js :options) :token)))
                              (send js :close)
                              val)`, natsURL),
		Expect: `"xyz"`,
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :token t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectTokenHandler(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), nil)
	defer func() {
		_ = slip.ReadString("(send js :close)").Eval(scope, nil)
	}()
	(&sliptest.Function{
		Scope: scope,
		Source: fmt.Sprintf(`(setq js
                                   (jet-connect :url %q
                                                :user "u1" :password "password"
                                                :token-handler (lambda (c) "toker")))`,
			natsURL),
		Expect: "/#<jet-client [0-9a-f]+>/",
	}).Test(t)
	inst, ok := scope.Get("js").(*flavors.Instance)
	tt.Equal(t, true, ok)
	nc := inst.Any.(*jet.Client).NatsConn()
	tt.NotNil(t, nc.Opts.TokenHandler)
	result := nc.Opts.TokenHandler()
	tt.Equal(t, "toker", result)
}

func TestClientConnectBadURL(t *testing.T) {
	(&sliptest.Function{
		Source:    `(jet-connect :url t :user "u1" :password "password")`,
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectUseOldRequestStyle(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"
                                                     :use-old-request-style 7))
                                    (val (get (send js :options) :use-old-request-style)))
                              (send js :close)
                              val)`, natsURL),
		Expect: "t",
	}).Test(t)
}

func TestClientConnectBadUser(t *testing.T) {
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user t :password "password")`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

// TBD nats: user callback defined without a signature handler
// func TestClientConnectUserJWTOk(t *testing.T) {
// 	scope := slip.NewScope()
// 	scope.Let(slip.Symbol("js"), nil)
// 	defer func() {
// 		_ = slip.ReadString("(send js :close)").Eval(scope, nil)
// 	}()
// 	(&sliptest.Function{
// 		Scope: scope,
// 		Source: fmt.Sprintf(`(setq js
//                                    (jet-connect :url %q
//                                                 :user "u1" :password "password"
//                                                 :user-jwt (lambda (b) "jit")))`,
// 			natsURL),
// 		Expect: "/#<jet-client [0-9a-f]+>/",
// 	}).Test(t)
// 	inst, ok := scope.Get("js").(*flavors.Instance)
// 	tt.Equal(t, true, ok)
// 	nc := inst.Any.(*jet.Client).NatsConn()
// 	tt.NotNil(t, nc.Opts.SignatureCB)
// 	jwt, err := nc.Opts.SignatureCB([]byte{'x'})
// 	tt.Nil(t, err)
// 	tt.Equal(t, "jit", jwt)
// }

func TestClientConnectUserJWTError(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), nil)
	(&sliptest.Function{
		Scope: scope,
		Source: fmt.Sprintf(`(jet-connect :url %q
                                          :user "u1" :password "password"
                                          :user-jwt (lambda () (panic "error")))`,
			natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}

func TestClientConnectVerbose(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q
                                                     :user "u1" :password "password"
                                                     :verbose 7))
                                    (value (get (send js :options) :verbose)))
                              (send js :close)
                              value)`, natsURL),
		Expect: "t",
	}).Test(t)
}

// TBD :trace
//  trace when pub sub is implemented

// TBD :publish-async-error-handler

func TestClientConnectPublishAsyncMaxPending(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q
                                                     :user "u1" :password "password"
                                                     :publish-async-max-pending 37))
                                    (value (get (send js :options) :publish-async-max-pending)))
                              (send js :close)
                              value)`, natsURL),
		Expect: "37",
	}).Test(t)
	(&sliptest.Function{
		Source: fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :publish-async-max-pending t)`,
			natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestClientConnectPrefix(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q
                                                     :user "u1" :password "password"
                                                     :prefix "test"))
                                    (value (get (send js :options) :prefix)))
                              (send js :close)
                              value)`, natsURL),
		Expect: `"test"`,
	}).Test(t)
	(&sliptest.Function{
		Source:    fmt.Sprintf(`(jet-connect :url %q :user "u1" :password "password" :prefix t)`, natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}
