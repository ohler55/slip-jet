// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestClientConsumerOk(t *testing.T) {
	defer cleanupTestStream("get-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "get-test" :subjects '("test.get.>")))
                                    (consumer (send jss :create-consumer :timeout 0.1 :name "eater"))
                                    (found (send js :consumer "get-test" "eater" :timeout 0.1))
                                    (not-found (send js :consumer "get-test" "no-one")))
                              (send js :close)
                              (list not-found
                                    (send consumer :equal found)
                                    (when found (send found :name))))`, natsURL),
		Expect: `(nil t "eater")`,
	}).Test(t)
}

func TestClientConsumerError(t *testing.T) {
	defer cleanupTestStream("get-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "get-test" :subjects '("test.get.>"))))
                              (send js :consumer "get-test" "get bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
