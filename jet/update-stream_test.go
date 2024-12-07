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

func TestUpdateStreamOk(t *testing.T) {
	options := nats.Options{
		Url:      natsURL,
		User:     "u1",
		Password: "password",
	}
	nc, err := options.Connect()
	tt.Nil(t, err)
	js, _ := jetstream.New(nc)
	defer func() { _ = js.DeleteStream(context.Background(), "update-test") }()

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "update-test"
                                                                 :subjects '("test.update")))
                                    (jus (send js :update-stream "update-test"
                                                                 :subjects '("test.update.>")
                                                                 :timeout 0.1))
                                    (result (list (send jus :name) (send jus :subjects))))
                              (send js :close)
                              result)`, natsURL),
		Expect: `("update-test" ("test.update.>"))`,
	}).Test(t)
}

func TestUpdateStreamError(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password")))
                              (send js :update-stream "update bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
