// Copyright (c) 2024, Peter Ohler, All rights reserved.

package jet_test

import (
	"fmt"
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestStreamNameBySubjectOk(t *testing.T) {
	defer cleanupTestStream("quux")

	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password"))
                                    (jss (send js :create-stream "quux" :subjects '("test.quux.>")))
                                    (name (send js :stream-name-by-subject "test.quux.bar" :timeout 0.1)))
                              (send js :close)
                              name)`, natsURL),
		Expect: `"quux"`,
	}).Test(t)
}

func TestStreamNameBySubjectNotFound(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password")))
                              (send js :stream-name-by-subject "test.quux"))`, natsURL),
		Expect: "nil",
	}).Test(t)
}

func TestStreamNameBySubjectError(t *testing.T) {
	(&sliptest.Function{
		Source: fmt.Sprintf(`(let* ((js (jet-connect :url %q :user "u1" :password "password")))
                              (send js :stream-name-by-subject "test quux"))`, natsURL),
		PanicType: slip.ErrorSymbol,
	}).Test(t)
}
