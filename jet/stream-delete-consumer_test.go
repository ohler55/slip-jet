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

func TestStreamDeleteConsumerOk(t *testing.T) {
	options := nats.Options{
		Url:      natsURL,
		User:     "u1",
		Password: "password",
	}
	nc, err := options.Connect()
	tt.Nil(t, err)
	js, _ := jetstream.New(nc)
	defer func() { _ = js.DeleteStream(context.Background(), "delete-test") }()

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "delete-test" :subjects '("test.delete.>")))
                                    (consumer (send jss :create-consumer :timeout 0.1 :name "eater"))
                                    result)
                              (send jss :delete-consumer "eater" :timeout 0.1)
                              (setq result (send jss :consumer-names))
                              (send js :close)
                              result)`, natsURL),
		Expect: "nil",
	}).Test(t)
}

func TestStreamDeleteConsumerError(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "delete-test" :subjects '("test.delete.>"))))
                              (send jss :delete-consumer "delete bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
