// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestCreateOrUpdateStreamOk(t *testing.T) {
	defer cleanupTestStream("create-or-update-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-or-update-stream "create-or-update-test"
                                                                           :subjects '("test.>")
                                                                           :timeout 0.1))
                                    (result (list (send jss :name) (send jss :subjects))))
                              (send js :close)
                              result)`, natsURL),
		Expect: `("create-or-update-test" ("test.>"))`,
	}).Test(t)
}

func TestCreateOrUpdateStreamError(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password")))
                              (send js :create-or-update-stream "bad name"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
