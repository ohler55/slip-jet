// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamListConsumersOk(t *testing.T) {
	defer cleanupTestStream("list-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "list-test" :subjects '("test.list.>")))
                                    (consumer (send jss :create-consumer :timeout 0.1 :name "eater"))
                                    (found (mapcar (lambda (ci) (send ci :name))
                                           (send jss :list-consumers :timeout 0.1))))
                              (send js :close)
                              found)`, natsURL),
		Expect: `("eater")`,
	}).Test(t)
}

func TestStreamListConsumersError(t *testing.T) {
	defer cleanupTestStream("list-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "list-test" :subjects '("test.list.>"))))
                              (send js :close)
                              (send jss :list-consumers))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
