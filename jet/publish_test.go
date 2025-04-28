// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestPublishString(t *testing.T) {
	streamName := "publish-test"
	js, stream := createStream(t, streamName, "test.publish.>")
	defer func() { _ = js.DeleteStream(context.Background(), streamName) }()

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (ack (send js :publish "hello" "test.publish.string"
                                                           :retry-attempts 3
                                                           :retry-wait 0.1
                                                           :timeout 1.0)))
                              (send js :close)
                              (list (send ack :stream-name) (send ack :sequence-number)))`, natsURL),
		Expect: `("publish-test" 1)`,
	}).Test(t)

	// Not really needed but verify the message can be consumed and has the
	// expected content.
	var msg jetstream.Msg

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{})
	tt.Nil(t, err)
	msg, err = cons.Next(jetstream.FetchMaxWait(time.Second))
	tt.Nil(t, err)
	tt.Equal(t, "test.publish.string", msg.Subject())
	tt.Equal(t, "hello", string(msg.Data()))
}

func TestPublishOctets(t *testing.T) {
	streamName := "publish-test"
	js, _ := createStream(t, streamName, "test.publish.>")
	defer func() { _ = js.DeleteStream(context.Background(), streamName) }()

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (ack (send js :publish (coerce "hello" 'octets) "test.publish.octets")))
                              (send js :close)
                              (list (send ack :stream-name) (send ack :sequence-number)))`, natsURL),
		Expect: `("publish-test" 1)`,
	}).Test(t)
}

func TestPublishMsg(t *testing.T) {
	streamName := "publish-test"
	js, _ := createStream(t, streamName, "test.publish.>")
	defer func() { _ = js.DeleteStream(context.Background(), streamName) }()

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (m (make-instance 'jet-msg :data "hello" :subject "test.publish.msg"))
                                    (ack (send js :publish m)))
                              (send js :close)
                              (list (send ack :stream-name) (send ack :sequence-number)))`, natsURL),
		Expect: `("publish-test" 1)`,
	}).Test(t)
}

func TestPublishExpectSeq(t *testing.T) {
	streamName := "lastseq-test"
	js, _ := createStream(t, streamName, "test.lastseq.>")
	defer func() { _ = js.DeleteStream(context.Background(), streamName) }()

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
		_ = slip.ReadString("(send js :close)", scope).Eval(scope, nil)
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
		_ = slip.ReadString("(send js :close)", scope).Eval(scope, nil)
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
		_ = slip.ReadString("(send js :close)", scope).Eval(scope, nil)
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
