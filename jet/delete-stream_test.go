// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestDeleteStream(t *testing.T) {
	defer cleanupTestStream("delete-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "delete-test" :subjects '("test.delete.>")))
                                    names)
                              (send js :delete-stream "delete-test" :timeout 0.1)
                              (setq names (send js :stream-names))
                              (send js :close)
                              names)`, natsURL),
		Expect: "nil",
	}).Test(t)
}

func TestDeleteStreamError(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password")))
                              (send js :delete-stream "not-a-stream"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
