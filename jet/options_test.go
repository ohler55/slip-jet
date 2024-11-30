// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip/sliptest"
)

func TestClientOptionsNil(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password")))
                              (send js :close)
                              (send js :options))`, natsURL),
		Expect: "nil",
	}).Test(t)
}
