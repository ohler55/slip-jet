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

func TestGetStreamOk(t *testing.T) {
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
                                    (found (send js :get-stream "get-test" :timeout 0.1)))
                              (send js :close)
                              (send found :name))`, natsURL),
		Expect: `"get-test"`,
	}).Test(t)
}

func TestGetStreamError(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password")))
                              (send js :get-stream "not-a-stream"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
