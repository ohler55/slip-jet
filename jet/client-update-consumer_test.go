// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestClientUpdateConsumerOk(t *testing.T) {
	defer cleanupTestStream("update-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "update-test" :subjects '("test.update.>")))
                                    (consumer (send js :create-consumer "update-test" :name "eater"))
                                    (updated (send js :update-consumer "update-test" :timeout 0.1 :name "eater"))
                                    (name (send updated :name)))
                              (send js :close)
                              (send consumer :equal updated))`, natsURL),
		Expect: "t",
	}).Test(t)
}

func TestClientUpdateConsumerError(t *testing.T) {
	defer cleanupTestStream("update-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "update-test" :subjects '("test.update.>"))))
                              (send js :update-consumer "update bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
