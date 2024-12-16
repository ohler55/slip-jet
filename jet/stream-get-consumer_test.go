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

func TestStreamGetConsumerOk(t *testing.T) {
	options := nats.Options{
		Url:      natsURL,
		User:     "u1",
		Password: "password",
	}
	nc, err := options.Connect()
	tt.Nil(t, err)
	js, _ := jetstream.New(nc)
	defer func() { _ = js.DeleteStream(context.Background(), "get-test") }()

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "get-test" :subjects '("test.get.>")))
                                    (consumer (send jss :create-consumer :timeout 0.1 :name "eater"))
                                    (found (send jss :get-consumer "eater" :timeout 0.1))
                                    (not-found (send jss :get-consumer "no-one")))
                              (send js :close)
                              (list not-found (when found (send found :name))))`, natsURL),
		Expect: `(nil "eater")`,
	}).Test(t)
}

func TestStreamGetConsumerError(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "get-test" :subjects '("test.get.>"))))
                              (send jss :get-consumer "get bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
