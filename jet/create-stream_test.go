// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestCreateStreamOk(t *testing.T) {
	defer cleanupTestStream("create-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "create-test"
                                                                 :subjects '("test.create.>")
                                                                 :timeout 0.1))
                                    (name (send jss :name)))
                              (send js :close)
                              (list name jss))`, natsURL),
		Expect: `/("create-test" #<jet-stream [0-9a-f]+>)/`,
	}).Test(t)
}

func TestCreateStreamError(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password")))
                              (send js :create-stream "create bad"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
