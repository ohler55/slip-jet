// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestPublishAsyncString(t *testing.T) {
	options := nats.Options{
		Url:      natsURL,
		User:     "u1",
		Password: "password",
	}
	nc, err := options.Connect()
	tt.Nil(t, err)
	js, _ := jetstream.New(nc)
	cfg := jetstream.StreamConfig{
		Name:     "async-string-test",
		Subjects: []string{"test.async.string.>"},
	}
	cfg.Storage = jetstream.FileStorage
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var stream jetstream.Stream
	stream, err = js.CreateStream(ctx, cfg)
	tt.Nil(t, err)
	tt.NotNil(t, stream)

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (future (send js :publish-async "hello" "test.async.string.pub" :stall-wait 0.5))
                                    (ack (send future :result)))
                              (send js :close)
                              (list (send ack :stream-name) (send ack :sequence-number)))`, natsURL),
		Expect: `("async-string-test" 1)`,
	}).Test(t)
}

func TestPublishAsyncOctets(t *testing.T) {
	options := nats.Options{
		Url:      natsURL,
		User:     "u1",
		Password: "password",
	}
	nc, err := options.Connect()
	tt.Nil(t, err)
	js, _ := jetstream.New(nc)
	cfg := jetstream.StreamConfig{
		Name:     "async-octets-test",
		Subjects: []string{"test.async.octets.>"},
	}
	cfg.Storage = jetstream.FileStorage
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var stream jetstream.Stream
	stream, err = js.CreateStream(ctx, cfg)
	tt.Nil(t, err)
	tt.NotNil(t, stream)

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (future (send js :publish-async (coerce "hello" 'octets) "test.async.octets.pub"))
                                    (ack (send future :result)))
                              (send js :close)
                              (list (send ack :stream-name) (send ack :sequence-number)))`, natsURL),
		Expect: `("async-octets-test" 1)`,
	}).Test(t)
}

func TestPublishAsyncMsg(t *testing.T) {
	options := nats.Options{
		Url:      natsURL,
		User:     "u1",
		Password: "password",
	}
	nc, err := options.Connect()
	tt.Nil(t, err)
	js, _ := jetstream.New(nc)
	cfg := jetstream.StreamConfig{
		Name:     "async-msg-test",
		Subjects: []string{"test.async.msg.>"},
	}
	cfg.Storage = jetstream.FileStorage
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var stream jetstream.Stream
	stream, err = js.CreateStream(ctx, cfg)
	tt.Nil(t, err)
	tt.NotNil(t, stream)

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (m (make-instance 'jet-msg :data "hello" :subject "test.async.msg.pub"))
                                    (future (send js :publish-async m))
                                    (ack (send future :result)))
                              (send js :close)
                              (list (send ack :stream-name) (send ack :sequence-number)))`, natsURL),
		Expect: `("async-msg-test" 1)`,
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
