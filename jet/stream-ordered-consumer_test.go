// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamOrderedConsumerOk(t *testing.T) {
	options := nats.Options{
		Url:      natsURL,
		User:     "u1",
		Password: "password",
	}
	nc, err := options.Connect()
	tt.Nil(t, err)
	js, _ := jetstream.New(nc)
	defer func() { _ = js.DeleteStream(context.Background(), "ordered-test") }()

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "ordered-test" :subjects '("test.ordered.>")))
                                    (consumer (send jss :ordered-consumer :timeout 0.1
                                                        :filter-subjects '("test.ordered.one" "test.ordered.two")))
                                    (filters (get (send (send consumer :info :cached t) :config) :filter-subjects)))
                              (send js :close)
                              (list filters consumer))`, natsURL),
		Expect: `/\(\("test.ordered.one" "test.ordered.two"\) #<jet-consumer [0-9a-f]+>\)/`,
	}).Test(t)
}

func TestStreamOrderedConsumerError(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "ordered-test" :subjects '("test.ordered.>"))))
                              (send jss :ordered-consumer :filter-subjects '("ordered bad")))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
