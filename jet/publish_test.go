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

func TestPublishString(t *testing.T) {
	// Setup for the publish in go to reduce dependencies on the lisp
	// functions in the testing phase.
	options := nats.Options{
		Url:      natsURL,
		User:     "u1",
		Password: "password",
	}
	nc, err := options.Connect()
	tt.Nil(t, err)
	js, _ := jetstream.New(nc)
	cfg := jetstream.StreamConfig{
		Name:     "string-test",
		Subjects: []string{"test.string.>"},
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
                                    (ack (send js :publish "hello" "test.string.pub"
                                                           :retry-attempts 3
                                                           :retry-wait 0.1
                                                           :timeout 1.0)))
                              (send js :close)
                              (list (send ack :stream-name) (send ack :sequence-number)))`, natsURL),
		Expect: `("string-test" 1)`,
	}).Test(t)

	// Not really needed but verify the message can be consumed and has the
	// expected content.
	var (
		cons jetstream.Consumer
		msg  jetstream.Msg
	)
	cons, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{})
	tt.Nil(t, err)
	msg, err = cons.Next(jetstream.FetchMaxWait(time.Second))
	tt.Nil(t, err)
	tt.Equal(t, "test.string.pub", msg.Subject())
	tt.Equal(t, "hello", string(msg.Data()))
}

func TestPublishOctets(t *testing.T) {
	// Setup for the publish in go to reduce dependencies on the lisp
	// functions in the testing phase.
	options := nats.Options{
		Url:      natsURL,
		User:     "u1",
		Password: "password",
	}
	nc, err := options.Connect()
	tt.Nil(t, err)
	js, _ := jetstream.New(nc)
	cfg := jetstream.StreamConfig{
		Name:     "octets-test",
		Subjects: []string{"test.octets.>"},
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
                                    (ack (send js :publish (coerce "hello" 'octets) "test.octets.pub")))
                              (send js :close)
                              (list (send ack :stream-name) (send ack :sequence-number)))`, natsURL),
		Expect: `("octets-test" 1)`,
	}).Test(t)
}

func TestPublishMsg(t *testing.T) {
	// Setup for the publish in go to reduce dependencies on the lisp
	// functions in the testing phase.
	options := nats.Options{
		Url:      natsURL,
		User:     "u1",
		Password: "password",
	}
	nc, err := options.Connect()
	tt.Nil(t, err)
	js, _ := jetstream.New(nc)
	cfg := jetstream.StreamConfig{
		Name:     "msg-test",
		Subjects: []string{"test.msg.>"},
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
                                    (m (make-instance 'jet-msg :data "hello" :subject "test.msg.pub"))
                                    (ack (send js :publish m)))
                              (send js :close)
                              (list (send ack :stream-name) (send ack :sequence-number)))`, natsURL),
		Expect: `("msg-test" 1)`,
	}).Test(t)
}

func TestPublishExpectSeq(t *testing.T) {
	// Setup for the publish in go to reduce dependencies on the lisp
	// functions in the testing phase.
	options := nats.Options{
		Url:      natsURL,
		User:     "u1",
		Password: "password",
	}
	nc, err := options.Connect()
	tt.Nil(t, err)
	js, _ := jetstream.New(nc)
	cfg := jetstream.StreamConfig{
		Name:     "lastseq-test",
		Subjects: []string{"test.lastseq.>"},
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
                                    (ack (send js :publish "hello" "test.lastseq.pub"
                                                           :msg-id "h1"
                                                           :expect-last-sequence 0)))
                              (send js :close)
                              (list (send ack :stream-name) (send ack :sequence-number)))`, natsURL),
		Expect: `("lastseq-test" 1)`,
	}).Test(t)
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (ack (send js :publish "hello" "test.lastseq.pub"
                                                           :expect-stream "lastseq-test"
                                                           :expect-last-msg-id "h1"
                                                           :expect-last-subject-sequence 1)))
                              (send js :close)
                              (list (send ack :stream-name) (send ack :sequence-number)))`, natsURL),
		Expect: `("lastseq-test" 2)`,
	}).Test(t)
}

func TestPublishBadPayload(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), nil)
	defer func() {
		_ = slip.ReadString("(send js :close)").Eval(scope, nil)
	}()
	(&sliptest.Function{
		Scope: scope,
		Source: fmt.Sprintf(`(send (setq js (jet-connect :url %q :user "u1" :password "password")) :publish t)`,
			natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
	(&sliptest.Function{
		Scope: scope,
		Source: fmt.Sprintf(`(send (setq js (jet-connect :url %q :user "u1" :password "password"))
                                   :publish (make-instance 'vanilla-flavor))`,
			natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestPublishBadArgType(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), nil)
	defer func() {
		_ = slip.ReadString("(send js :close)").Eval(scope, nil)
	}()
	(&sliptest.Function{
		Scope: scope,
		Source: fmt.Sprintf(`(send
                              (setq js (jet-connect :url %q :user "u1" :password "password"))
                              :publish "x" :retry-wait t)`,
			natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
	(&sliptest.Function{
		Scope: scope,
		Source: fmt.Sprintf(`(send
                              (setq js (jet-connect :url %q :user "u1" :password "password"))
                              :publish "x" :retry-attempts t)`,
			natsURL),
		PanicType: slip.TypeErrorSymbol,
	}).Test(t)
}

func TestPublishFail(t *testing.T) {
	scope := slip.NewScope()
	scope.Let(slip.Symbol("js"), nil)
	defer func() {
		_ = slip.ReadString("(send js :close)").Eval(scope, nil)
	}()
	(&sliptest.Function{
		Scope: scope,
		Source: fmt.Sprintf(`(send
                              (setq js (jet-connect :url %q :user "u1" :password "password"))
                              :publish "x" :subject "bad")`,
			natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
