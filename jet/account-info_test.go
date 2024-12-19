// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestAccountInfoOk(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (info (send js :account-info :timeout 0.1)))
                              (send js :close)
                              (list (class-name (class-of info)) (send info :has "limits")))`, natsURL),
		Expect: "(bag-flavor t)",
	}).Test(t)
}

func TestAccountInfoError(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password")))
                              (send js :close)
                              (send js :account-info))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
