// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestGetStreamOk(t *testing.T) {
	defer cleanupTestStream("get-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "get-test" :subjects '("test.get.>")))
                                    (found (send js :stream "get-test" :timeout 0.1))
                                    (not-found (send js :stream "not-a-stream")))
                              (send js :close)
                              (list (send found :name) not-found))`, natsURL),
		Expect: `("get-test" nil)`,
	}).Test(t)
}

func TestGetStreamError(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password")))
                              (send js :stream "not a stream"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
