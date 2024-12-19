// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestClientDeleteConsumerOk(t *testing.T) {
	defer cleanupTestStream("delete-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "delete-test" :subjects '("test.delete.>")))
                                    (consumer (send jss :create-consumer :timeout 0.1 :name "eater"))
                                    result)
                              (send js :delete-consumer "delete-test" "eater" :timeout 0.1)
                              (setq result (send jss :consumer-names))
                              (send js :close)
                              result)`, natsURL),
		Expect: "nil",
	}).Test(t)
}

func TestClientDeleteConsumerError(t *testing.T) {
	defer cleanupTestStream("delete-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "delete-test" :subjects '("test.delete.>"))))
                              (send js :delete-consumer "delete-test" "delete bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
