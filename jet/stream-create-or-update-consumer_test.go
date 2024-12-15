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

func TestStreamCreateOrUpdateConsumerOk(t *testing.T) {
	options := nats.Options{
		Url:      natsURL,
		User:     "u1",
		Password: "password",
	}
	nc, err := options.Connect()
	tt.Nil(t, err)
	js, _ := jetstream.New(nc)
	defer func() { _ = js.DeleteStream(context.Background(), "create-or-update-test") }()

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-or-update-stream "create-or-update-test"
                                                  :subjects '("test.create-or-update.>")))
                                    (consumer (send jss :create-or-update-consumer :timeout 0.1 :name "eater"))
                                    (name (send consumer :name)))
                              (send js :close)
                              (list name consumer))`, natsURL),
		Expect: `/("eater" #<jet-consumer [0-9a-f]+>)/`,
	}).Test(t)
}

func TestStreamCreateOrUpdateConsumerError(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-or-update-stream "create-or-update-test" :subjects '("test.create-or-update.>"))))
                              (send jss :create-or-update-consumer :name "create-or-update bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
