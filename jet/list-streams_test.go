// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip/sliptest"
)

func TestListStreamOk(t *testing.T) {
	options := nats.Options{
		Url:      natsURL,
		User:     "u1",
		Password: "password",
	}
	nc, err := options.Connect()
	tt.Nil(t, err)
	js, _ := jetstream.New(nc)
	defer func() { _ = js.DeleteStream(context.Background(), "list-test") }()

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "list-test" :subjects '("test.list.>")))
                                    (info (send js :list-streams :timeout 0.1)))
                              (send js :close)
                              info)`, natsURL),
		Expect: `nil`, // TBD
	}).Test(t)
}

// func TestListStreamError(t *testing.T) {
// 	(&sliptest.Function{
// 		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password")))
//                               (send js :list-stream "not-a-stream"))`, natsURL),
// 		PanicType: slip.ErrorSymbol,
// 	}).Test(t)
// }
