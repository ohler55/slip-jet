// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip/sliptest"
)

func TestStreamNamesOk(t *testing.T) {
	defer cleanupTestStream("names-test")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "names-test" :subjects '("test.list.>")))
                                    (names (send js :stream-names :timeout 0.1 :subject "test.list.x")))
                              (send js :close)
                              names)`, natsURL),
		Expect: `("names-test")`,
	}).Test(t)
}
